package tenantcostclient

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var saveDebtCSV = flag.String(
	"save-debt-csv", "",
	"save latency data from TestTokenBucketDebt to a csv file",
)

func TestLimiter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	start := timeutil.Now()
	ts := timeutil.NewManualTime(start)

	ch := make(chan struct{}, 100)

	var tb tokenBucket
	tb.Init(ts.Now(), ch, 10 /* rate */, 100 /* available */)

	check := func(expected string) {
		// Call AvailableRU to trigger time update.
		t.Helper()
		tb.AvailableTokens(ts.Now())
		actual := tb.String()
		if actual != expected {
			t.Errorf("expected: %s\nactual: %s\n", expected, actual)
		}
	}
	check("100.00 RU filling @ 10.00 RU/s (avg 100.00 RU/op)")

	// Verify basic update.
	ts.Advance(1 * time.Second)
	check("110.00 RU filling @ 10.00 RU/s (avg 100.00 RU/op)")
	// Check RemoveRU.
	tb.RemoveTokens(ts.Now(), 200)
	check("-90.00 RU filling @ 10.00 RU/s (avg 100.00 RU/op)")

	ts.Advance(1 * time.Second)
	check("-80.00 RU filling @ 10.00 RU/s (avg 100.00 RU/op)")
	ts.Advance(15 * time.Second)
	check("70.00 RU filling @ 10.00 RU/s (avg 100.00 RU/op)")

	fulfill := func(amount tenantcostmodel.RU) {
		t.Helper()
		if ok, _ := tb.TryToFulfill(ts.Now()); !ok {
			t.Fatalf("failed to fulfill")
		}
		tb.RemoveTokens(ts.Now(), amount)
	}
	fulfill(100)
	check("-30.00 RU filling @ 10.00 RU/s (avg 100.00 RU/op)")

	// TryAgainAfter should be the time to pay off entire debt.
	if ok, tryAgainAfter := tb.TryToFulfill(ts.Now()); ok {
		t.Fatalf("fulfilled incorrectly")
	} else if exp := 3 * time.Second; tryAgainAfter.Round(time.Millisecond) != exp {
		t.Fatalf("tryAgainAfter: expected %s, got %s", exp, tryAgainAfter)
	}
	check("-30.00 RU filling @ 10.00 RU/s (avg 300.00 RU/op)")

	// Create massive debt and ensure that delay is = maxTryAgainAfterSeconds.
	tb.RemoveTokens(ts.Now(), maxTryAgainAfterSeconds*10)
	if ok, tryAgainAfter := tb.TryToFulfill(ts.Now()); ok {
		t.Fatalf("fulfilled incorrectly")
	} else if exp := maxTryAgainAfterSeconds * time.Second; tryAgainAfter.Round(time.Millisecond) != exp {
		t.Fatalf("tryAgainAfter: expected %s, got %s", exp, tryAgainAfter)
	}
	check("-10030.00 RU filling @ 10.00 RU/s (avg 300.00 RU/op)")
	ts.Advance(maxTryAgainAfterSeconds * time.Second)

	// Update average RU/op in order to test case where TryAgainAfter should be
	// the time to pay off debt in excess of maxDebtSeconds.
	tb.avgRUPerOp = 3
	tb.opRU = 0
	tb.opCount = 0
	if ok, tryAgainAfter := tb.TryToFulfill(ts.Now()); ok {
		t.Fatalf("fulfilled incorrectly")
	} else if exp := 1 * time.Second; tryAgainAfter.Round(time.Millisecond) != exp {
		t.Fatalf("tryAgainAfter: expected %s, got %s", exp, tryAgainAfter)
	}
	check("-30.00 RU filling @ 10.00 RU/s (avg 3.00 RU/op)")

	// Wait for 1 second in order to pay off excess debt. Ensure that operations
	// can be fulfilled every 300 ms from then on.
	ts.Advance(1 * time.Second)
	check("-20.00 RU filling @ 10.00 RU/s (avg 3.00 RU/op)")
	fulfill(3)
	ts.Advance(300 * time.Millisecond)
	fulfill(3)
	ts.Advance(300 * time.Millisecond)
	fulfill(3)
	check("-23.00 RU filling @ 10.00 RU/s (avg 3.00 RU/op)")

	// Check notification.
	checkNoNotification := func() {
		t.Helper()
		select {
		case <-ch:
			t.Error("unexpected notification")
		default:
		}
	}

	checkNotification := func() {
		t.Helper()
		select {
		case <-ch:
		default:
			t.Error("expected notification")
		}
	}

	checkNoNotification()
	args := tokenBucketReconfigureArgs{
		NewTokens:       43,
		NewRate:         10,
		NotifyThreshold: 5,
	}
	tb.Reconfigure(ts.Now(), args)

	checkNoNotification()
	ts.Advance(1 * time.Second)
	check("30.00 RU filling @ 10.00 RU/s (avg 3.00 RU/op)")
	fulfill(20)
	// No notification: we did not go below the threshold.
	checkNoNotification()
	// Now we should get a notification.
	fulfill(8)
	checkNotification()
	check("2.00 RU filling @ 10.00 RU/s (avg 3.00 RU/op)")

	// We only get one notification (until we Reconfigure or StartNotification).
	fulfill(1)
	checkNoNotification()

	args = tokenBucketReconfigureArgs{
		NewTokens: 80,
		NewRate:   1,
	}
	tb.Reconfigure(ts.Now(), args)
	check("81.00 RU filling @ 1.00 RU/s (avg 3.00 RU/op)")
	ts.Advance(1 * time.Second)

	checkNoNotification()
	tb.SetupNotification(ts.Now(), 50)
	checkNoNotification()
	fulfill(60)
	checkNotification()
	check("22.00 RU filling @ 1.00 RU/s (avg 4.33 RU/op)")

	// Test exponential moving average RU/op.
	fulfill(60)
	ts.Advance(100 * time.Second)
	check("62.00 RU filling @ 1.00 RU/s (avg 15.47 RU/op)")
}

// TestTokenBucketDebt simulates a closed-loop workload with parallelism 1 in
// combination with incurring a fixed amount of debt every second. It verifies
// that queue times are never too high, and optionally emits all queue time
// information into a csv file.
func TestLimiterDebt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	start := timeutil.Now()
	ts := timeutil.NewManualTime(start)

	var lim limiter
	lim.Init(ts, nil /* notifyChan */)
	lim.Reconfigure(limiterReconfigureArgs{NewRate: 100})

	const tickDuration = time.Millisecond
	const debtPeriod = time.Second
	const debtTicks = int(debtPeriod / tickDuration)
	const totalTicks = 10 * debtTicks
	const debt tenantcostmodel.RU = 50

	const reqRUMean = 2.0
	const reqRUStdDev = reqRUMean / 2.0

	// Use a fixed seed so the result is reproducible.
	r := rand.New(rand.NewSource(1234))
	randRu := func() tenantcostmodel.RU {
		ru := r.NormFloat64()*reqRUStdDev + reqRUMean
		if ru < 0 {
			return 0
		}
		return tenantcostmodel.RU(ru)
	}

	out := io.Discard
	if *saveDebtCSV != "" {
		file, err := os.Create(*saveDebtCSV)
		if err != nil {
			t.Fatalf("error creating csv file: %v", err)
		}
		defer func() {
			if err := file.Close(); err != nil {
				t.Errorf("error closing csv file: %v", err)
			}
		}()
		out = file
	}

	var req *waitRequest
	reqTick := 0
	opsCount := 0
	ctx := context.Background()

	fmt.Fprintf(out, "time (s), RUs requested, queue latency (ms), debt applied (RU), available RUs, extra RUs\n")
	for tick := 0; tick < totalTicks; tick++ {
		var d tenantcostmodel.RU
		if tick > 0 && tick%debtTicks == 0 {
			d = debt
			lim.OnTick(d)
			opsCount = 0
		}

		if req != nil {
			if tenantcostmodel.RU(req.needed) <= lim.AvailableRU() {
				// This should not block, since we checked above there are enough
				// tokens.
				if err := lim.qp.Acquire(ctx, req); err != nil {
					t.Fatalf("error during Acquire: %v", err)
				}

				opsCount++
				latency := tick - reqTick
				if latency > 100 {
					// A single request took longer than 100ms; we did a poor job smoothing
					// out the debt.
					t.Fatalf("high latency for request: %d ms", latency)
				}
				fmt.Fprintf(out, "%8.3f,%14.2f,%19d,%18.1f,%14.2f,%10.2f\n",
					(time.Duration(tick) * tickDuration).Seconds(),
					req.needed, latency, d, lim.AvailableRU(), lim.extraUsage())

				req = nil
			} else {
				fmt.Fprintf(out, "%8.3f,              ,                   ,%18.1f,%14.2f,%10.2f\n",
					(time.Duration(tick) * tickDuration).Seconds(), d, lim.AvailableRU(), lim.extraUsage())
			}
		}

		if req == nil {
			// Create a request now.
			req = &waitRequest{needed: quotapool.Tokens(lim.amortizeExtraRU(randRu()))}
			reqTick = tick
		}

		ts.Advance(tickDuration)
	}
}
