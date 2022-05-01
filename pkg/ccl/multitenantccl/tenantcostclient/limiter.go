// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantcostclient

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const maxTryAgainAfter = 1000 * time.Second

// limiter is used to rate-limit KV requests according to a local token bucket.
//
// The Wait() method is called when a KV request arrives. If the token bucket is
// low on RUs, then the request may need to be delayed.
//
// The other methods are used to adjust/reconfigure/replenish the local token
// bucket.
type limiter struct {
	// Immutable fields.
	notifyChan chan struct{}

	// These fields are only updated

	mu struct {
		sync.Mutex
		extraUsage   tenantcostmodel.RU
		opsCount     float64
		opsPerTick   float64
		extraRUPerOp tenantcostmodel.RU
	}

	// Access to this field is protected by the quota pool lock, not the mutex.
	qp struct {
		*quotapool.AbstractPool
		tb              quotapool.TokenBucket
		notifyThreshold quotapool.Tokens
	}
}

// limiterReconfigureArgs is used to update the limiter's configuration,
// including its underlying token bucket.
type limiterReconfigureArgs struct {
	// NewTokens is the number of tokens that should be added to the limiter's
	// token bucket.
	NewTokens tenantcostmodel.RU

	// NewRate is the new token accumulation rate for the limiter's token bucket.
	NewRate tenantcostmodel.RU

	// NotifyThreshold is the token bucket level below which a low RU notification
	// will be sent.
	NotifyThreshold tenantcostmodel.RU
}

// movingAvgOpsPerSecFactor is the weight applied to new average ops/sec
// samples. The lower this is, the "smoother" the moving average will be.
const movingAvgOpsPerSecFactor = 0.2

func (l *limiter) Init(timeSource timeutil.TimeSource, notifyChan chan struct{}) {
	*l = limiter{
		notifyChan: notifyChan,
	}

	onWaitFinishFn := func(ctx context.Context, poolName string, r quotapool.Request, start time.Time) {
		// Log a trace event for requests that waited for a long time.
		if waitDuration := timeSource.Since(start); waitDuration > time.Second {
			log.VEventf(ctx, 1, "request waited for RUs for %s", waitDuration.String())
		}
	}

	l.qp.AbstractPool = quotapool.New(
		"tenant-side-limiter", l,
		quotapool.WithTimeSource(timeSource),
		quotapool.OnWaitFinish(onWaitFinishFn),
	)

	// The token bucket is a bit awkward, with no built-in support for an
	// unlimited burst value, and also forcing current = burst when initialized.
	// Use MaxFloat64 as burst to approximate an unlimited burst value. Then
	// subtract out MaxFloat64 from the current value in order to start it at
	// zero.
	l.qp.tb.Init(0 /* rate */, quotapool.Tokens(math.MaxFloat64), timeSource)
	l.qp.tb.Adjust(-quotapool.Tokens(math.MaxFloat64))
}

func (l *limiter) Close() {
	l.qp.Close("shutting down")
}

// Wait blocks until the needed RUs are available in the bucket. This is called
// before making a read/write request. Note that Wait does not actually remove
// the RUs from the bucket. Instead, RemoveRU should be called once the
// read/write response has been received.
func (l *limiter) Wait(ctx context.Context, needed tenantcostmodel.RU) error {
	if needed > 0 {
		// Combine the requested RUs with some portion of any outstanding extraRUs.
		needed = l.amortizeExtraRU(needed)
	}

	r := newWaitRequest(quotapool.Tokens(needed))
	defer putWaitRequest(r)

	return l.qp.Acquire(ctx, r)
}

func (l *limiter) amortizeExtraRU(needed tenantcostmodel.RU) tenantcostmodel.RU {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.mu.opsCount++

	extraRU := l.mu.extraRUPerOp
	if extraRU > l.mu.extraUsage {
		extraRU = l.mu.extraUsage
	}

	needed += extraRU
	l.mu.extraUsage -= extraRU

	return needed
}

// RemoveRU removes tokens from the bucket. This is called when accounting
// for CPU usage or when a read/write response has been received (i.e.
// isReadWrite is true).
func (l *limiter) RemoveRU(amount tenantcostmodel.RU) {
	l.qp.Update(func(res quotapool.Resource) (shouldNotify bool) {
		l.qp.tb.Adjust(quotapool.Tokens(-amount))
		l.maybeNotifyLocked(l.qp.tb.Current())

		// Don't notify the head of the queue; this change can only delay the time
		// it can go through.
		return false
	})
}

func (l *limiter) OnTick(extraUsage tenantcostmodel.RU) {
	// Access locked fields within protected function. Don't attempt to update
	// the quota pool in this function to reduce risk of deadlocks caused by
	// different lock acquisition orders.
	removeRU := func() (removeRU tenantcostmodel.RU) {
		l.mu.Lock()
		defer l.mu.Unlock()

		if l.mu.opsCount == 0 {
			// During this last tick, there have been no operations to associate
			// with extra RU usage. Remove all previous usage directly from the
			// token bucket without attempting to distribute it across operations.
			removeRU = l.mu.extraUsage
			l.mu.extraUsage = 0
		}

		// Add extra usage during the last tick.
		l.mu.extraUsage += extraUsage

		// Distribute the extra usage across the estimated number of operations
		// occurring during the next tick.
		if l.mu.opsPerTick == 0 {
			l.mu.opsPerTick = l.mu.opsCount
		} else {
			// Update the exponential moving average of operations per tick.
			l.mu.opsPerTick = movingAvgOpsPerSecFactor*l.mu.opsCount +
				(1-movingAvgOpsPerSecFactor)*l.mu.opsPerTick
		}
		l.mu.opsCount = 0

		// Compute how any extra RUs will be divided amongst the expected
		// number of operations in the next tick.
		opsPerTick := l.mu.opsPerTick
		if opsPerTick < 1 {
			// Add all extra RUs to the next operation.
			opsPerTick = 1
		}
		l.mu.extraRUPerOp = l.mu.extraUsage / tenantcostmodel.RU(opsPerTick)

		return removeRU
	}()

	l.RemoveRU(removeRU)
}

// Reconfigure is used to call tokenBucket.Reconfigure under the pool's lock.
func (l *limiter) Reconfigure(cfg limiterReconfigureArgs) {
	l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
		l.qp.tb.UpdateConfig(quotapool.TokensPerSecond(cfg.NewRate), quotapool.Tokens(math.MaxFloat64))
		l.qp.tb.Adjust(quotapool.Tokens(cfg.NewTokens))
		l.qp.notifyThreshold = quotapool.Tokens(cfg.NotifyThreshold)
		l.maybeNotifyLocked(l.qp.tb.Current())

		// Notify the head of the queue; the new configuration might allow that
		// request to go through earlier.
		return true
	})
}

// AvailableRU returns the current number of available RUs. This can be
// negative if we accumulated debt.
func (l *limiter) AvailableRU() tenantcostmodel.RU {
	var result tenantcostmodel.RU
	l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
		result = tenantcostmodel.RU(l.qp.tb.Current())
		return false
	})
	return result
}

// SetupNotification is used to call tokenBucket.SetupNotification under the
// pool's lock.
func (l *limiter) SetupNotification(threshold tenantcostmodel.RU) {
	l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
		l.qp.notifyThreshold = quotapool.Tokens(threshold)
		l.maybeNotifyLocked(l.qp.tb.Current())
		return false
	})
}

// extraUsage is an unexported function used by a unit test.
func (l *limiter) extraUsage() tenantcostmodel.RU {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.mu.extraUsage
}

func (l *limiter) String() string {
	l.mu.Lock()
	avg := l.mu.opsPerTick
	extraUsage := l.mu.extraUsage
	l.mu.Unlock()

	var s string
	l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
		s = fmt.Sprintf("%.2f RU filling @ %.2f RU/s (avg %.2f ops/sec, %.2f extra RU)",
			l.qp.tb.Current(), l.qp.tb.Rate(), avg, extraUsage)
		return false
	})
	return s
}

// maybeNotify checks if it's time to send the notification and if so, performs
// the notification.

// notify tries to send a non-blocking notification on notifyCh and disables
// further notifications (until the next Reconfigure or StartNotification).

// Must be called in scope of qp lock
func (l *limiter) maybeNotifyLocked(current quotapool.Tokens) {
	if l.qp.notifyThreshold > 0 && current < l.qp.notifyThreshold {
		// The channel send is non-blocking, so OK to perform in scope of the lock.
		l.qp.notifyThreshold = 0
		select {
		case l.notifyChan <- struct{}{}:
		default:
		}
	}
}

// waitRequest is used to wait for adequate resources in the tokenBucket.
type waitRequest struct {
	needed quotapool.Tokens
}

var _ quotapool.Request = (*waitRequest)(nil)

var waitRequestSyncPool = sync.Pool{
	New: func() interface{} { return new(waitRequest) },
}

// newWaitRequest allocates a waitRequest from the sync.Pool.
// It should be returned with putWaitRequest.
func newWaitRequest(needed quotapool.Tokens) *waitRequest {
	r := waitRequestSyncPool.Get().(*waitRequest)
	*r = waitRequest{needed: needed}
	return r
}

func putWaitRequest(r *waitRequest) {
	*r = waitRequest{}
	waitRequestSyncPool.Put(r)
}

// Acquire is part of quotapool.Request.
func (req *waitRequest) Acquire(
	ctx context.Context, res quotapool.Resource,
) (fulfilled bool, tryAgainAfter time.Duration) {
	l := res.(*limiter)

	// Always fulfill the request unless the token bucket is in debt.
	current := l.qp.tb.Current()
	if current >= 0 {
		current -= req.needed
		l.qp.tb.Adjust(-req.needed)
		fulfilled = true
	} else {
		// Let the token bucket calculate tryAgainAfter.
		fulfilled, tryAgainAfter = l.qp.tb.TryToFulfill(req.needed)
		if fulfilled {
			current = l.qp.tb.Current()
		} else if l.qp.tb.Rate() == 0 || tryAgainAfter > maxTryAgainAfter {
			// Handle zero rate by returning a large tryAgainAfter value.
			// TODO(andyk): Fix this in TryToFulfill, which does not check for
			// overflow when converting from float to duration.
			tryAgainAfter = maxTryAgainAfter
		}
	}

	l.maybeNotifyLocked(current)

	return fulfilled, tryAgainAfter
}

// ShouldWait is part of quotapool.Request.
func (req *waitRequest) ShouldWait() bool {
	return true
}
