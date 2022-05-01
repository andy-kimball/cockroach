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
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/errorspb"
)

// TargetPeriodSetting is exported for testing purposes.
var TargetPeriodSetting = settings.RegisterDurationSetting(
	settings.TenantReadOnly,
	"tenant_cost_control_period",
	"target duration between token bucket requests from tenants (requires restart)",
	10*time.Second,
	checkDurationInRange(5*time.Second, 30*time.Second),
)

// CPUUsageAllowance is exported for testing purposes.
var CPUUsageAllowance = settings.RegisterDurationSetting(
	settings.TenantReadOnly,
	"tenant_cpu_usage_allowance",
	"this much CPU usage per second is considered background usage and "+
		"doesn't contribute to consumption; for example, if it is set to 10ms, "+
		"that corresponds to 1% of a CPU",
	10*time.Millisecond,
	checkDurationInRange(0, 1000*time.Millisecond),
)

// checkDurationInRange returns a function used to validate duration cluster
// settings. Because these values are currently settable by the tenant, we need
// to restrict the allowed values to avoid possible sabotage of the cost control
// mechanisms.
func checkDurationInRange(min, max time.Duration) func(v time.Duration) error {
	return func(v time.Duration) error {
		if v < min || v > max {
			return errors.Errorf("value %s out of range (%s, %s)", v, min, max)
		}
		return nil
	}
}

// Initial settings for the local token bucket. They are used only until the
// first TokenBucket request returns. We allow immediate use of the initial RUs
// (we essentially borrow them and pay them back in the first TokenBucket
// request). The intention is to avoid any throttling during start-up in normal
// circumstances.
const initialRUs = 10000
const initialRate = 100

// mainLoopUpdateInterval is the period at which we collect CPU usage and
// evaluate whether we need to send a new token request.
const mainLoopUpdateInterval = 1 * time.Second

// movingAvgRUPerSecFactor is the weight applied to a new "sample" of RU usage
// (with one sample per mainLoopUpdateInterval).
//
// If we want a factor of 0.5 per second, this should be:
//   0.5^(1 second / mainLoopUpdateInterval)
const movingAvgRUPerSecFactor = 0.5

// We request more tokens when the available RUs go below a threshold. The
// threshold is a fraction of the last granted RUs.
const notifyFraction = 0.1

// When we trickle RUs over a period of time, we request more tokens a bit
// before that period runs out. This "anticipation" should be more than what we
// expect the RTT of a token bucket request to be in practice. It should also be
// longer than mainLoopUpdateInterval, since we only check it on each tick.
const anticipation = 2 * time.Second

// If we have less than this many RUs to report, extend the reporting period to
// reduce load on the host cluster.
const consumptionReportingThreshold = 100

// The extended reporting period is this factor times the normal period.
const extendedReportingPeriodFactor = 4

// We try to maintain this many RUs in our local bucket, regardless of estimated
// usage. This is intended to support usage spikes without blocking.
const bufferRUs = 5000

func newTenantSideCostController(
	st *cluster.Settings,
	tenantID roachpb.TenantID,
	provider kvtenant.TokenBucketProvider,
	timeSource timeutil.TimeSource,
	testInstr TestInstrumentation,
) (multitenant.TenantSideCostController, error) {
	if tenantID == roachpb.SystemTenantID {
		return nil, errors.AssertionFailedf("cost controller can't be used for system tenant")
	}
	c := &tenantSideCostController{
		timeSource:      timeSource,
		testInstr:       testInstr,
		settings:        st,
		tenantID:        tenantID,
		provider:        provider,
		responseChan:    make(chan *roachpb.TokenBucketResponse, 1),
		lowRUNotifyChan: make(chan struct{}, 1),
	}
	c.limiter.Init(timeSource, c.lowRUNotifyChan)
	c.limiter.Reconfigure(limiterReconfigureArgs{NewRate: initialRate, NewTokens: initialRUs})

	c.costCfg = tenantcostmodel.ConfigFromSettings(&st.SV)
	return c, nil
}

// NewTenantSideCostController creates an object which implements the
// server.TenantSideCostController interface.
func NewTenantSideCostController(
	st *cluster.Settings, tenantID roachpb.TenantID, provider kvtenant.TokenBucketProvider,
) (multitenant.TenantSideCostController, error) {
	return newTenantSideCostController(
		st, tenantID, provider,
		timeutil.DefaultTimeSource{},
		nil, /* testInstr */
	)
}

// TestingTenantSideCostController is a testing variant of
// NewTenantSideCostController which allows using a specified TimeSource.
func TestingTenantSideCostController(
	st *cluster.Settings,
	tenantID roachpb.TenantID,
	provider kvtenant.TokenBucketProvider,
	timeSource timeutil.TimeSource,
	testInstr TestInstrumentation,
) (multitenant.TenantSideCostController, error) {
	return newTenantSideCostController(st, tenantID, provider, timeSource, testInstr)
}

// TestingTokenBucketString returns a string representation of the tenant's
// token bucket, for testing purposes.
func TestingTokenBucketString(ctrl multitenant.TenantSideCostController) string {
	c := ctrl.(*tenantSideCostController)
	return c.limiter.String()
}

func init() {
	server.NewTenantSideCostController = NewTenantSideCostController
}

type tenantSideCostController struct {
	timeSource           timeutil.TimeSource
	testInstr            TestInstrumentation
	settings             *cluster.Settings
	costCfg              tenantcostmodel.Config
	tenantID             roachpb.TenantID
	provider             kvtenant.TokenBucketProvider
	limiter              limiter
	stopper              *stop.Stopper
	instanceID           base.SQLInstanceID
	sessionID            sqlliveness.SessionID
	externalUsageFn      multitenant.ExternalUsageFn
	nextLiveInstanceIDFn multitenant.NextLiveInstanceIDFn

	mu struct {
		syncutil.Mutex

		consumption roachpb.TenantConsumption
	}

	// lowRUNotifyChan is used when the number of available RUs is running low and
	// we need to send an early token bucket request.
	lowRUNotifyChan chan struct{}

	// responseChan is used to receive results from token bucket requests, which
	// are run in a separate goroutine. A nil response indicates an error.
	responseChan chan *roachpb.TokenBucketResponse

	// run contains the state that is updated by the main loop. It doesn't need a
	// mutex since the main loop runs on a single goroutine.
	run struct {
		lastTick time.Time
		// externalUsage stores the last value returned by externalUsageFn.
		externalUsage multitenant.ExternalUsage
		// consumption stores the last value of mu.consumption.
		consumption roachpb.TenantConsumption
		// requestSeqNum is an increasing sequence number that is included in token
		// bucket requests.
		requestSeqNum int64

		// targetPeriod stores the value of the TargetPeriodSetting setting at the
		// last update.
		targetPeriod time.Duration

		// initialRequestCompleted is set to true when the first token bucket
		// request completes successfully.
		initialRequestCompleted bool

		// requestInProgress is true if we are in the process of sending a request;
		// it gets set to false when we process the response (in the main loop),
		// even in error cases.
		requestInProgress *roachpb.TokenBucketRequest

		// shouldSendRequest is set if the last token bucket request encountered an
		// error. This triggers a retry attempt on the next tick.
		//
		// Note: shouldSendRequest and requestInProgress are never true/non-nil at
		// the same time.
		shouldSendRequest bool

		lastRequestTime         time.Time
		lastReportedConsumption roachpb.TenantConsumption

		lastRate         float64
		trickleDeadline  time.Time
		trickleThreshold tenantcostmodel.RU
		hasLowRUs        bool

		// fallbackRate is the refill rate we fall back to if the token bucket
		// requests don't complete or take a long time.
		fallbackRate float64

		// avgRUPerSec is an exponentially-weighted moving average of the RU
		// consumption per second; used to estimate the RU requirements for the next
		// request.
		avgRUPerSec float64
		// lastSecRU is the consumption.RU value when avgRUPerSec was last updated.
		avgRUPerSecLastRU float64
	}
}

var _ multitenant.TenantSideCostController = (*tenantSideCostController)(nil)

// Start is part of multitenant.TenantSideCostController.
func (c *tenantSideCostController) Start(
	ctx context.Context,
	stopper *stop.Stopper,
	instanceID base.SQLInstanceID,
	sessionID sqlliveness.SessionID,
	externalUsageFn multitenant.ExternalUsageFn,
	nextLiveInstanceIDFn multitenant.NextLiveInstanceIDFn,
) error {
	if instanceID == 0 {
		return errors.New("invalid SQLInstanceID")
	}
	if sessionID == "" {
		return errors.New("invalid sqlliveness.SessionID")
	}
	c.stopper = stopper
	c.instanceID = instanceID
	c.sessionID = sessionID
	c.externalUsageFn = externalUsageFn
	c.nextLiveInstanceIDFn = nextLiveInstanceIDFn
	return stopper.RunAsyncTask(ctx, "cost-controller", func(ctx context.Context) {
		c.mainLoop(ctx)
	})
}

func (c *tenantSideCostController) initRunState(ctx context.Context) {
	c.run.targetPeriod = TargetPeriodSetting.Get(&c.settings.SV)

	now := c.timeSource.Now()
	c.run.lastTick = now
	c.run.externalUsage = c.externalUsageFn(ctx)
	c.run.lastRequestTime = now
	c.run.avgRUPerSec = initialRUs / c.run.targetPeriod.Seconds()
	c.run.requestSeqNum = 1
}

// onTick is called whenever the main loop awakens, in order to account
// for CPU and egress usage in the interim.
func (c *tenantSideCostController) onTick(ctx context.Context, newTime time.Time) {
	newExternalUsage := c.externalUsageFn(ctx)

	// Update CPU consumption.
	deltaCPU := newExternalUsage.CPUSecs - c.run.externalUsage.CPUSecs

	// Subtract any allowance that we consider free background usage.
	if deltaTime := newTime.Sub(c.run.lastTick); deltaTime > 0 {
		allowance := CPUUsageAllowance.Get(&c.settings.SV).Seconds() * deltaTime.Seconds()
		deltaCPU -= allowance

		// If total CPU usage is small (less than 3% of a single CPU by default)
		// and there have been no recent read/write operations, then ignore the
		// recent usage altogether. This is intended to minimize RU usage when the
		// cluster is idle.
		c.mu.Lock()
		if deltaCPU < allowance*2 {
			if c.mu.consumption.ReadBatches == c.run.consumption.ReadBatches &&
				c.mu.consumption.WriteBatches == c.run.consumption.WriteBatches {
				deltaCPU = 0
			}
		}
		c.mu.Unlock()
	}
	if deltaCPU < 0 {
		deltaCPU = 0
	}

	ru := tenantcostmodel.RU(deltaCPU) * c.costCfg.PodCPUSecond

	var deltaPGWireEgressBytes uint64
	if newExternalUsage.PGWireEgressBytes > c.run.externalUsage.PGWireEgressBytes {
		deltaPGWireEgressBytes = newExternalUsage.PGWireEgressBytes - c.run.externalUsage.PGWireEgressBytes
		ru += tenantcostmodel.RU(deltaPGWireEgressBytes) * c.costCfg.PGWireEgressByte
	}

	// KV RUs are not included here, these metrics correspond only to the SQL pod.
	c.mu.Lock()
	c.mu.consumption.SQLPodsCPUSeconds += deltaCPU
	c.mu.consumption.PGWireEgressBytes += deltaPGWireEgressBytes
	c.mu.consumption.RU += float64(ru)
	newConsumption := c.mu.consumption
	c.mu.Unlock()

	elapsed := newTime.Sub(c.run.lastTick)
	c.run.lastTick = newTime
	c.run.externalUsage = newExternalUsage
	c.run.consumption = newConsumption

	// Update the average RUs consumed per second, based on the latest stats.
	c.updateAvgRUPerSec(elapsed)

	// Notify the limiter that another tick has elapsed, passing it the extra
	// usage accumulated during the tick.
	c.limiter.OnTick(ru)

	// If last request came with a trickle duration, then just before it
	// ends, configure the low RU notification so that another token bucket
	// request will be triggered if/when the bucket gets low.
	if c.run.trickleThreshold > 0 && !c.recentlyStartedTrickle() {
		c.limiter.SetupNotification(c.run.trickleThreshold)
		c.run.trickleThreshold = 0
	}

	// Should a token bucket request be sent? It might be for a retry or for
	// periodic consumption reporting.
	if c.run.shouldSendRequest || c.shouldReportConsumption(newTime) {
		c.sendTokenBucketRequest(ctx)
	}
}

// updateAvgRUPerSec is called exactly once per mainLoopUpdateInterval. The
// elapsed parameter gives the amount of time that has passed since the last
// time this was computed.
func (c *tenantSideCostController) updateAvgRUPerSec(elapsed time.Duration) {
	delta := c.run.consumption.RU - c.run.avgRUPerSecLastRU
	avg := delta * float64(time.Second) / float64(elapsed)
	c.run.avgRUPerSec = movingAvgRUPerSecFactor*avg + (1-movingAvgRUPerSecFactor)*c.run.avgRUPerSec
	c.run.avgRUPerSecLastRU = c.run.consumption.RU
}

// shouldReportConsumption decides if it's time to send a token bucket request
// to report consumption.
func (c *tenantSideCostController) shouldReportConsumption(now time.Time) bool {
	timeSinceLastRequest := now.Sub(c.run.lastRequestTime)
	if timeSinceLastRequest >= c.run.targetPeriod {
		consumptionToReport := c.run.consumption.RU - c.run.lastReportedConsumption.RU
		if consumptionToReport >= consumptionReportingThreshold {
			return true
		}
		if timeSinceLastRequest >= extendedReportingPeriodFactor*c.run.targetPeriod {
			return true
		}
	}

	return false
}

// recentlyStartedTrickle returns true if a trickle is in progress but has not
// gotten near its deadline (i.e. the point at which the trickle should be
// extended). If this is true, then additional RUs should not be requested from
// the token bucket server.
func (c *tenantSideCostController) recentlyStartedTrickle() bool {
	return c.run.lastTick.Before(c.run.trickleDeadline.Add(-anticipation))
}

func (c *tenantSideCostController) sendTokenBucketRequest(ctx context.Context) {
	if c.run.requestInProgress != nil {
		// Don't allow multiple concurrent token bucket requests. But do send
		// another request once the in-progress request completes.
		c.run.shouldSendRequest = true
		return
	}
	c.run.shouldSendRequest = false

	deltaConsumption := c.run.consumption
	deltaConsumption.Sub(&c.run.lastReportedConsumption)

	var requested float64

	if !c.run.initialRequestCompleted {
		requested = initialRUs
	} else if c.recentlyStartedTrickle() {
		// Don't request additional RUs if we're in the middle of a trickle
		// that was started recently.
		requested = 0
	} else {
		// Request what we expect to need over the next target period plus the
		// buffer amount.
		requested = c.run.avgRUPerSec*c.run.targetPeriod.Seconds() + bufferRUs

		// Adjust by the currently available amount. If we are in debt, we request
		// more to cover the debt.
		requested -= float64(c.limiter.AvailableRU())
		if requested < 0 {
			// We don't need more RUs right now, but we still want to report
			// consumption.
			requested = 0
		}
	}

	req := &roachpb.TokenBucketRequest{
		TenantID:                    c.tenantID.ToUint64(),
		InstanceID:                  uint32(c.instanceID),
		InstanceLease:               c.sessionID.UnsafeBytes(),
		NextLiveInstanceID:          uint32(c.nextLiveInstanceIDFn(ctx)),
		SeqNum:                      c.run.requestSeqNum,
		ConsumptionSinceLastRequest: deltaConsumption,
		RequestedRU:                 requested,
		TargetRequestPeriod:         c.run.targetPeriod,
	}
	c.run.requestInProgress = req
	c.run.requestSeqNum++

	c.run.lastRequestTime = c.run.lastTick
	c.run.lastReportedConsumption = c.run.consumption

	ctx, _ = c.stopper.WithCancelOnQuiesce(ctx)
	err := c.stopper.RunAsyncTask(ctx, "token-bucket-request", func(ctx context.Context) {
		if log.ExpensiveLogEnabled(ctx, 1) {
			log.Infof(ctx, "TokenBucket request: %s\n", req.String())
		}
		resp, err := c.provider.TokenBucket(ctx, req)
		if err != nil {
			// Don't log any errors caused by the stopper canceling the context.
			if !errors.Is(err, context.Canceled) {
				log.Warningf(ctx, "TokenBucket RPC error: %v", err)
			}
			resp = nil
		} else if (resp.Error != errorspb.EncodedError{}) {
			// This is a "logic" error which indicates a configuration problem on the
			// host side. We will keep retrying periodically.
			err := errors.DecodeError(ctx, resp.Error)
			log.Warningf(ctx, "TokenBucket error: %v", err)
			resp = nil
		}
		c.responseChan <- resp
	})
	if err != nil {
		// We are shutting down and could not send the request.
		c.responseChan <- nil
	}
}

func (c *tenantSideCostController) handleTokenBucketResponse(
	ctx context.Context, req *roachpb.TokenBucketRequest, resp *roachpb.TokenBucketResponse,
) {
	if log.ExpensiveLogEnabled(ctx, 1) {
		log.Infof(
			ctx, "TokenBucket response: %g RUs over %s (fallback rate %g)",
			resp.GrantedRU, resp.TrickleDuration, resp.FallbackRate,
		)
	}
	c.run.fallbackRate = resp.FallbackRate

	// Don't process granted RUs if none were requested.
	if req.RequestedRU == 0 {
		return
	}

	// Process granted RUs.
	granted := resp.GrantedRU
	if !c.run.trickleDeadline.IsZero() {
		// If last request came with a trickle duration, we may have RUs that were
		// not made available to the bucket yet; throw them together with the newly
		// granted RUs.
		// NB: There is a race condition here, where the token bucket can consume
		// tokens between the time we call Now() and the time we reconfigure the
		// bucket below. This would result in double usage of the same granted
		// tokens. However, this is not a big concern, since it's a small window,
		// and even if it occurs, the usage is still counted. The only effect is
		// some extra debt accumulation, which is fine.
		if since := c.run.trickleDeadline.Sub(c.timeSource.Now()); since > 0 {
			granted += c.run.lastRate * since.Seconds()
		}
	}

	// Calculate the threshold at which a low RU notification will be sent.
	notifyThreshold := tenantcostmodel.RU(granted * notifyFraction)
	if notifyThreshold < bufferRUs && granted > 0 {
		notifyThreshold = bufferRUs
	}

	// Directly add tokens to the bucket if they're immediately available.
	// Configure a token trickle if the tokens are only available over time. If
	// zero tokens were granted, then set up a zero token trickle that will not
	// send a low RU notification until the trickle deadline is near (to avoid
	// spamming server token bucket if it's empty).
	var cfg limiterReconfigureArgs
	if granted > 0 && resp.TrickleDuration == 0 {
		// We received a batch of tokens to use as needed. Set up the token bucket
		// to notify us when the tokens are running low.
		cfg.NewTokens = tenantcostmodel.RU(granted)
		// TODO(radu): if we don't get more tokens in time, fall back to a "fallback"
		// rate.
		cfg.NewRate = 0
		cfg.NotifyThreshold = notifyThreshold

		c.run.trickleDeadline = time.Time{}
		c.run.trickleThreshold = 0
	} else {
		// We received a batch of tokens that can only be used over the
		// TrickleDuration. Set up the token bucket to notify us a bit before this
		// period elapses (unless we accumulate enough unused tokens, in which case
		// we get notified when the tokens are running low).
		c.run.trickleDeadline = c.run.lastTick.Add(resp.TrickleDuration)
		c.run.trickleThreshold = notifyThreshold

		if granted > 0 {
			cfg.NewRate = tenantcostmodel.RU(granted / resp.TrickleDuration.Seconds())
		}
	}
	c.run.lastRate = float64(cfg.NewRate)
	c.limiter.Reconfigure(cfg)

	// Wait until reconfigure is done before removing the initial RUs to avoid
	// triggering an unnecessary low RU notification.
	if !c.run.initialRequestCompleted {
		c.run.initialRequestCompleted = true
		// This is the first successful request. Take back the initial RUs that we
		// used to pre-fill the bucket.
		c.limiter.RemoveRU(initialRUs)
	}

	if log.ExpensiveLogEnabled(ctx, 1) {
		log.Infof(ctx, "Limiter: %s", c.limiter.String())
	}
}

func (c *tenantSideCostController) mainLoop(ctx context.Context) {
	interval := mainLoopUpdateInterval
	// Make sure the interval is never larger than the target request period. This
	// is useful for tests which set a very small period.
	if targetPeriod := TargetPeriodSetting.Get(&c.settings.SV); targetPeriod < interval {
		interval = targetPeriod
	}
	ticker := c.timeSource.NewTicker(interval)
	defer ticker.Stop()
	tickerCh := ticker.Ch()

	c.initRunState(ctx)
	c.sendTokenBucketRequest(ctx)

	// The main loop should never block. The remote requests run in separate
	// goroutines.
	for {
		select {
		case <-tickerCh:
			// If ticks are delayed, or we're slow in receiving, they can get backed
			// up. Discard any ticks which are received too quickly in succession.
			// Note that we're deliberately not using the time received from tickerCh
			// because it might cause logic bugs when developers assume it is
			// consistent with timeSource.Now (e.g. by computing extreme averages
			// due to tiny intervals between ticks).
			now := c.timeSource.Now()
			if now.Before(c.run.lastTick.Add(interval / 2)) {
				break
			}

			c.onTick(ctx, now)
			if c.testInstr != nil {
				c.testInstr.Event(now, TickProcessed)
			}

		case resp := <-c.responseChan:
			req := c.run.requestInProgress
			c.run.requestInProgress = nil
			if resp != nil {
				// Token bucket request was successful.
				c.handleTokenBucketResponse(ctx, req, resp)

				// Immediately send another token bucket request if one was requested
				// while this one was in progress.
				if c.run.shouldSendRequest {
					c.sendTokenBucketRequest(ctx)
				}

				if c.testInstr != nil {
					c.testInstr.Event(c.timeSource.Now(), TokenBucketResponseProcessed)
				}
			} else {
				// A nil response indicates a failure (which would have been logged).
				// Retry the request on the next tick so there's at least some
				// delay between retries.
				c.run.shouldSendRequest = true

				// Switch to the fallback rate if RUs are low.
				if c.run.fallbackRate > 0 && c.run.hasLowRUs {
					log.Infof(ctx, "switching to fallback rate %.10g", c.run.fallbackRate)
					c.limiter.Reconfigure(limiterReconfigureArgs{
						NewRate: tenantcostmodel.RU(c.run.fallbackRate),
					})
					c.run.fallbackRate = 0
				}
				if c.testInstr != nil {
					c.testInstr.Event(c.timeSource.Now(), TokenBucketResponseError)
				}
			}
			c.run.hasLowRUs = false

		case <-c.lowRUNotifyChan:
			c.run.hasLowRUs = true
			c.sendTokenBucketRequest(ctx)

			if c.testInstr != nil {
				c.testInstr.Event(c.timeSource.Now(), LowRUNotification)
			}

		case <-c.stopper.ShouldQuiesce():
			c.limiter.Close()
			// TODO(radu): send one last request to update consumption.
			return
		}
	}
}

// OnRequestWait is part of the multitenant.TenantSideKVInterceptor interface.
func (c *tenantSideCostController) OnRequestWait(ctx context.Context) error {
	if multitenant.HasTenantCostControlExemption(ctx) {
		return nil
	}

	// Note that the tenantSideController might not be started yet; that is ok
	// because we initialize the limiter with some initial RUs and a reasonable
	// initial rate.
	return c.limiter.Wait(ctx, 0)
}

// OnResponse is part of the multitenant.TenantSideBatchInterceptor interface.
func (c *tenantSideCostController) OnResponseWait(
	ctx context.Context, req tenantcostmodel.RequestInfo, resp tenantcostmodel.ResponseInfo,
) error {
	if multitenant.HasTenantCostControlExemption(ctx) {
		return nil
	}

	// Account for the cost of write requests and read responses.
	writeRU := c.costCfg.RequestCost(req)
	readRU := c.costCfg.ResponseCost(resp)
	totalRU := writeRU + readRU

	// TODO(andyk): Consider breaking up huge acquisition requests into chunks
	// that can be fulfilled separately and reported separately. This would make
	// it easier to stick within a constrained RU/s budget.
	if err := c.limiter.Wait(ctx, totalRU); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if req.IsWrite() {
		c.mu.consumption.WriteBatches++
		c.mu.consumption.WriteRequests += uint64(req.WriteCount())
		c.mu.consumption.WriteBytes += uint64(req.WriteBytes())
		c.mu.consumption.KVRU += float64(writeRU)
		c.mu.consumption.RU += float64(writeRU)
	} else if resp.IsRead() {
		c.mu.consumption.ReadBatches++
		c.mu.consumption.ReadRequests += uint64(resp.ReadCount())
		c.mu.consumption.ReadBytes += uint64(resp.ReadBytes())
		c.mu.consumption.KVRU += float64(readRU)
		c.mu.consumption.RU += float64(readRU)
	}

	return nil
}
