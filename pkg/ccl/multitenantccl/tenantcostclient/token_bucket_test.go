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
	"io"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestTokenBucket(t *testing.T) {
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

// TestTokenBucketTryToFulfill verifies that the tryAgainAfter time returned by
// TryToFulfill is consistent if recalculated after some time has passed.
func TestTokenBucketTryToFulfill(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r, _ := randutil.NewPseudoRand()
	randRU := func(min, max float64) tenantcostmodel.RU {
		return tenantcostmodel.RU(min + r.Float64()*(max-min))
	}
	randDuration := func(max time.Duration) time.Duration {
		return time.Duration(r.Intn(int(max + 1)))
	}

	start := timeutil.Now()
	const runs = 50000
	for run := 0; run < runs; run++ {
		var tb tokenBucket
		clock := timeutil.NewManualTime(start)
		tb.Init(clock.Now(), nil, randRU(1, 100), randRU(0, 500))

		// Advance a random amount of time.
		clock.Advance(randDuration(100 * time.Millisecond))

		if rand.Intn(5) > 0 {
			// Add some debt and advance more.
			tb.RemoveTokens(clock.Now(), randRU(1, 500))
			clock.Advance(randDuration(100 * time.Millisecond))
		}

		// Fulfill requests until we can't anymore.
		var ru tenantcostmodel.RU
		var tryAgainAfter time.Duration
		for {
			ru = randRU(0, 100)
			var ok bool
			ok, tryAgainAfter = tb.TryToFulfill(clock.Now())
			if !ok {
				break
			}
			tb.RemoveTokens(clock.Now(), ru)
		}
		if tryAgainAfter == maxTryAgainAfterSeconds*time.Second {
			// TryToFullfill has a cap; we cannot crosscheck the value if that cap is
			// hit.
			continue
		}
		state := tb
		// Now check that if we advance the time a bit, the tryAgainAfter time
		// agrees with the previous one.
		advance := randDuration(tryAgainAfter)
		clock.Advance(advance)
		ok, newTryAgainAfter := tb.TryToFulfill(clock.Now())
		if ok {
			newTryAgainAfter = 0
		}
		tb.RemoveTokens(clock.Now(), ru)
		// Check that the two calls agree on when the request can go through.
		diff := advance + newTryAgainAfter - tryAgainAfter
		const tolerance = 10 * time.Nanosecond
		if diff < -tolerance || diff > tolerance {
			t.Fatalf(
				"inconsistent tryAgainAfter\nstate: %+v\nru: %f\ntryAgainAfter: %s\ntryAgainAfter after %s: %s",
				state, ru, tryAgainAfter, advance, newTryAgainAfter,
			)
		}
	}
}

// TestTokenBucketDebt simulates a closed-loop workload with parallelism 1 in
// combination with incurring a fixed amount of debt every second. It verifies
// that queue times are never too high, and optionally emits all queue time
// information into a csv file.
func TestTokenBucketDebt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	start := timeutil.Now()
	ts := timeutil.NewManualTime(start)

	var tb tokenBucket
	tb.Init(ts.Now(), nil, 100 /* rate */, 0 /* available */)

	const tickDuration = time.Millisecond
	const debtPeriod = time.Second
	const debtTicks = int(debtPeriod / tickDuration)
	const totalTicks = 10 * debtTicks
	const debt tenantcostmodel.RU = 50

	const reqRUMean = 1.0
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

	ru := randRu()
	reqTick := 0

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

	fmt.Fprintf(out, "time (s),queue latency (ms),debt applied (RU)\n")
	for tick := 0; tick < totalTicks; tick++ {
		now := ts.Now()
		var d tenantcostmodel.RU
		if tick > 0 && tick%debtTicks == 0 {
			d = debt
			tb.RemoveTokens(now, debt)
		}
		if ok, _ := tb.TryToFulfill(now); ok {
			tb.RemoveTokens(now, ru)
			latency := tick - reqTick
			if latency > 100 {
				// A single request took longer than 100ms; we did a poor job smoothing
				// out the debt.
				t.Fatalf("high latency for request: %d ms", latency)
			}
			fmt.Fprintf(out, "%.3f,%3d, %.1f\n", (time.Duration(tick) * tickDuration).Seconds(), latency, d)
			ru = randRu()
			reqTick = tick
		} else {
			fmt.Fprintf(out, "%.3f,   , %.1f\n", (time.Duration(tick) * tickDuration).Seconds(), d)
		}
		ts.Advance(tickDuration)
	}
}
