// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantcostclient

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
)

// movingAvgRUPerOpFactor is the weight applied to a new KV operation cost
// "sample". The lower this is, the "smoother" the moving average will be.
const movingAvgRUPerOpFactor = 0.2

// defaultAvgRUPerOp is used before an average can be calculated (at least one
// second of data is needed).
const defaultAvgRUPerOp = 100

// Note that the Acquire implementation doesn't actually remove any tokens from
// the bucket. As long as the bucket is not in debt, all
// simply validates that the tokens are available. If they are not available,
// then it returns the estimated amount of time it will take to replenish them.

// tokenBucket implements a token bucket. It is a more specialized form of
// quotapool.TokenBucket. The main differences are:
//  - it does not currently support a burst limit;
//  - it implements a "low tokens" notification mechanism;
//  - it has special debt handling.
//
// -- Notification mechanism --
//
// Notifications can be configured to fire when the number of available RUs dips
// below a certain value (or when a request blocks). Notifications are delivered
// via a non-blocking send to a given channel.
//
// -- Debt handling --
//
// The token bucket is designed to handle arbitrary removal of tokens to account
// for usage that cannot be throttled (e.g. read/transferred bytes, CPU usage).
// This can bring the token bucket into debt.
//
// The simplest handling of debt is to always pay the debt first, blocking all
// operations that require tokens in the mean time. However, this is undesirable
// because the accounting can happen at arbitrary intervals which can lead to
// the workload periodically experiencing starvation (e.g. CPU usage might be
// accounted for only once per second, which can lead to all requests being
// blocked for the beginning part of each second).
//
// Instead, we aim to pay all outstanding debt D within time T from the time the
// last debt was incurred. We do this by splitting the refill rate into D/T and
// using only what's left for replenishing tokens.
//
// This rate is recalculated every time we incur debt. So in essence, every time
// we incur debt, we put it together with all existing debt and plan to pay it
// within time T (we can think of this as "refinancing" the existing debt).
//
// This behavior is somewhat arbitrary because the rate at which we pay debt
// depends on how frequently we incur additional debt. To see how much it can
// vary, imagine that at time t=0 we incur some debt D(0) and consider the two
// extremes:
//
//   A. We start with debt D(0), and we never recalculate the rate (no
//      "refinancing"). We pay debt at constant rate D(0) / T and all debt is
//      paid at time T.
//
//   B. We start with debt D(0), and we recalculate the rate ("refinance")
//      continuously (or, more intuitively, every nanosecond).  The
//      instantaneous rate is:
//        D'(t) = - D(t) / T
//      The debt formula is:
//        D(t) = D(0) * e^(-t/T)
//      We pay 63% of the debt in time T; 86% in 2T; and 95% in 3T.
//
// The difference between these two extremes is reasonable - we pay between 63%
// and 100% of the debt in time T, depending on the usage pattern.
//
// Design note: ideally we would always simulate B. However, under this model it
// is hard to compute the time until a request can go through (i.e. time until
// we accumulate a certain amount of tokens): it involves calculating the
// intersection of a line with an exponential, which cannot be solved
// algebraically (it requires slower numerical methods).
type tokenBucket struct {
	// -- Static fields --

	// Once the available RUs are below the notifyThreshold or a request cannot
	// be immediately fulfilled, we do a (non-blocking) send on this channel.
	notifyCh chan struct{}

	// -- Dynamic fields --
	// Protected by the AbstractPool's lock. All changes should happen either
	// inside a Request.Acquire() method or under AbstractPool.Update().

	// See notifyCh. A threshold of zero disables notifications.
	notifyThreshold tenantcostmodel.RU

	// Refill rate, in RU/s.
	rate tenantcostmodel.RU
	// Currently available RUs. Can not be negative.
	available tenantcostmodel.RU

	opCount       int64
	opRU          tenantcostmodel.RU
	lastAvgRUCalc time.Time

	// avgRUPerOp is an exponential moving average of the RU cost of a KV
	// operation, used to determine how long to wait when in debt.
	avgRUPerOp tenantcostmodel.RU

	lastFulfilled time.Time

	lastUpdated time.Time
}

func (tb *tokenBucket) Init(
	now time.Time, notifyCh chan struct{}, rate, available tenantcostmodel.RU,
) {
	*tb = tokenBucket{
		notifyCh:        notifyCh,
		notifyThreshold: 0,
		rate:            rate,
		available:       available,
		lastUpdated:     now,
	}
}

// update accounts for the passing of time.
func (tb *tokenBucket) update(now time.Time) {
	since := now.Sub(tb.lastUpdated)
	if since <= 0 {
		return
	}
	tb.lastUpdated = now
	sinceSeconds := since.Seconds()
	tb.available += tb.rate * tenantcostmodel.RU(sinceSeconds)
	tb.calculateAverageRUPerOp(now)
}

// calculateAverageRUPerOp maintains the exponential moving average RU cost per
// read/write operation. This value is used in order to "pace" fulfillment when
// the bucket is in debt.
func (tb *tokenBucket) calculateAverageRUPerOp(now time.Time) tenantcostmodel.RU {
	// In order to get a reasonable average, wait until at least one second has
	// elapsed and until there's been some activity.
	if now.Sub(tb.lastAvgRUCalc) >= time.Second && tb.opCount > 0 && tb.opRU > 0 {
		avg := tb.opRU / tenantcostmodel.RU(tb.opCount)

		// Update exponential moving average.
		if tb.avgRUPerOp == 0 {
			tb.avgRUPerOp = avg
		} else {
			tb.avgRUPerOp = movingAvgRUPerOpFactor*avg +
				(1-movingAvgRUPerOpFactor)*tb.avgRUPerOp
		}

		tb.opCount = 0
		tb.opRU = 0
		tb.lastAvgRUCalc = now
	}

	if tb.avgRUPerOp == 0 {
		return defaultAvgRUPerOp
	}
	return tb.avgRUPerOp
}

// notify tries to send a non-blocking notification on notifyCh and disables
// further notifications (until the next Reconfigure or StartNotification).
func (tb *tokenBucket) notify() {
	tb.notifyThreshold = 0
	select {
	case tb.notifyCh <- struct{}{}:
	default:
	}
}

// maybeNotify checks if it's time to send the notification and if so, performs
// the notification.
func (tb *tokenBucket) maybeNotify(now time.Time) {
	if tb.notifyThreshold > 0 && tb.available < tb.notifyThreshold {
		tb.notify()
	}
}

// RemoveTokens decreases the amount of tokens currently available. If there are
// not enough tokens, this causes the token bucket to go into debt.
func (tb *tokenBucket) RemoveTokens(now time.Time, amount tenantcostmodel.RU) {
	tb.update(now)
	tb.opRU += amount
	tb.available -= amount
	tb.maybeNotify(now)
}

type tokenBucketReconfigureArgs struct {
	NewTokens tenantcostmodel.RU

	NewRate tenantcostmodel.RU

	NotifyThreshold tenantcostmodel.RU
}

// Reconfigure changes the rate, optionally adjusts the available tokens and
// configures the next notification.
func (tb *tokenBucket) Reconfigure(now time.Time, args tokenBucketReconfigureArgs) {
	tb.update(now)
	// If we already produced a notification that wasn't processed, drain it. This
	// not racy as long as this code does not run in parallel with the code that
	// receives from notifyCh.
	select {
	case <-tb.notifyCh:
	default:
	}
	tb.rate = args.NewRate
	tb.available += args.NewTokens

	// Only reset notifications if additional tokens are available. Otherwise,
	// if the bucket is in debt and the server is out of tokens, we'll get into
	// a loop where a low RU notification triggers a token bucket request, which
	// returns 0 tokens, which triggers another notification, and so on.
	if args.NewTokens > 0 {
		tb.notifyThreshold = args.NotifyThreshold
		tb.maybeNotify(now)
	}
}

// SetupNotification enables the notification at the given threshold.
func (tb *tokenBucket) SetupNotification(now time.Time, threshold tenantcostmodel.RU) {
	tb.update(now)
	tb.notifyThreshold = threshold
}

const maxTryAgainAfterSeconds = 1000
const maxDebtSeconds = 2

// TryToFulfill either removes the given amount if is available, or returns a
// time after which the request should be retried.
func (tb *tokenBucket) TryToFulfill(now time.Time) (fulfilled bool, tryAgainAfter time.Duration) {
	tb.update(now)

	if tb.available < 0 {
		var delaySeconds float64

		// Compute number of seconds needed to pay off debt at current fill rate.
		debtSeconds := float64(-tb.available / tb.rate)
		if debtSeconds > maxDebtSeconds {
			// Delay further fulfillment until debt in excess of the max tolerated
			// amount is paid down.
			delaySeconds = debtSeconds - maxDebtSeconds
		}

		// Compute number of seconds to delay between operations, in an attempt
		// to balance RU consumption with the bucket fill rate. Subtract number
		// of seconds that have already elapsed since the last fulfillment.
		elapsedSeconds := float64(now.Sub(tb.lastFulfilled)) / float64(time.Second)
		averageRUPerOp := tb.calculateAverageRUPerOp(now)
		opDelaySeconds := float64(averageRUPerOp/tb.rate) - elapsedSeconds
		if opDelaySeconds > delaySeconds {
			// Estimated wait between operations is greater than the time to
			// pay excess debt.
			delaySeconds = opDelaySeconds
		}

		// Never wait longer than the time it would take to pay off all debt.
		if debtSeconds < delaySeconds {
			delaySeconds = debtSeconds
		}

		// Cap the number of seconds to avoid overflow; we want to tolerate
		// even a fill rate of 0 (in which case we are really waiting for a token
		// adjustment).
		if delaySeconds > maxTryAgainAfterSeconds {
			delaySeconds = maxTryAgainAfterSeconds
		}

		// Delay if it hasn't been long enough since the last fulfillment.
		tryAgainAfter = time.Duration(delaySeconds * float64(time.Second))
		if tryAgainAfter > 0 {
			if tryAgainAfter < time.Nanosecond {
				tryAgainAfter = time.Nanosecond
			}
			return false, tryAgainAfter
		}
	}

	tb.lastFulfilled = now
	tb.opCount++
	return true, 0
}

// AvailableTokens returns the current number of available RUs. This can be
// negative if we accumulated debt.
func (tb *tokenBucket) AvailableTokens(now time.Time) tenantcostmodel.RU {
	tb.update(now)
	return tb.available
}

func (tb *tokenBucket) String() string {
	return fmt.Sprintf("%.2f RU filling @ %.2f RU/s (avg %.2f RU/op)",
		tb.available, tb.rate, tb.calculateAverageRUPerOp(tb.lastAvgRUCalc))
}
