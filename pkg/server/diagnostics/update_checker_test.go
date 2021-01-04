// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package diagnostics_test

import (
	"context"
	"net/url"
	"runtime"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/diagnostics"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations"
	"github.com/cockroachdb/cockroach/pkg/testutils/diagutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/cloudinfo"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCheckVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer cloudinfo.Disable()()

	ctx := context.Background()

	t.Run("expected-reporting", func(t *testing.T) {
		r := diagutils.NewServer()
		defer r.Close()

		url := r.URL()
		s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DiagnosticsTestingKnobs: diagnostics.TestingKnobs{
						OverrideUpdatesURL: &url,
					},
				},
			},
		})
		defer s.Stopper().Stop(ctx)
		s.UpdateChecker().(*diagnostics.UpdateChecker).CheckForUpdates(ctx)
		r.Close()

		if expected, actual := 1, r.NumRequests(); actual != expected {
			t.Fatalf("expected %v update checks, got %v", expected, actual)
		}

		last := r.LastRequestData()
		if expected, actual := s.(*server.TestServer).ClusterID().String(), last.UUID; expected != actual {
			t.Errorf("expected uuid %v, got %v", expected, actual)
		}

		if expected, actual := build.GetInfo().Tag, last.Version; expected != actual {
			t.Errorf("expected version tag %v, got %v", expected, actual)
		}

		if expected, actual := "OSS", last.LicenseType; expected != actual {
			t.Errorf("expected license type %v, got %v", expected, actual)
		}

		if expected, actual := "false", last.Internal; expected != actual {
			t.Errorf("expected internal to be %v, got %v", expected, actual)
		}
	})

	t.Run("npe", func(t *testing.T) {
		// Ensure nil, which happens when an empty env override URL is used, does not
		// cause a crash.
		var nilURL *url.URL
		s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DiagnosticsTestingKnobs: diagnostics.TestingKnobs{
						OverrideUpdatesURL:   &nilURL,
						OverrideReportingURL: &nilURL,
					},
				},
			},
		})
		defer s.Stopper().Stop(ctx)
		s.UpdateChecker().(*diagnostics.UpdateChecker).CheckForUpdates(ctx)
		s.DiagnosticsReporter().(*diagnostics.Reporter).ReportDiagnostics(ctx)
	})
}

func TestUsageQuantization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer cloudinfo.Disable()()

	skip.UnderRace(t, "takes >1min under race")
	r := diagutils.NewServer()
	defer r.Close()

	st := cluster.MakeTestingClusterSettings()
	ctx := context.Background()

	url := r.URL()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings: st,
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DiagnosticsTestingKnobs: diagnostics.TestingKnobs{
					OverrideReportingURL: &url,
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)
	ts := s.(*server.TestServer)

	// Disable periodic reporting so it doesn't interfere with the test.
	if _, err := db.Exec(`SET CLUSTER SETTING diagnostics.reporting.enabled = false`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`SET application_name = 'test'`); err != nil {
		t.Fatal(err)
	}

	// Issue some queries against the test app name.
	for i := 0; i < 8; i++ {
		if _, err := db.Exec(`SELECT 1`); err != nil {
			t.Fatal(err)
		}
	}
	// Between 10 and 100 queries is quantized to 10.
	for i := 0; i < 30; i++ {
		if _, err := db.Exec(`SELECT 1,2`); err != nil {
			t.Fatal(err)
		}
	}
	// Between 100 and 10000 gets quantized to 100.
	for i := 0; i < 200; i++ {
		if _, err := db.Exec(`SELECT 1,2,3`); err != nil {
			t.Fatal(err)
		}
	}
	// Above 10000 gets quantized to 10000.
	for i := 0; i < 10010; i++ {
		if _, err := db.Exec(`SHOW application_name`); err != nil {
			t.Fatal(err)
		}
	}

	// Flush the SQL stat pool.
	ts.SQLServer().(*sql.Server).ResetSQLStats(ctx)

	// Collect a round of statistics.
	ts.DiagnosticsReporter().(*diagnostics.Reporter).ReportDiagnostics(ctx)

	// The stats "hide" the application name by hashing it. To find the
	// test app name, we need to hash the ref string too prior to the
	// comparison.
	clusterSecret := sql.ClusterSecret.Get(&st.SV)
	hashedAppName := sql.HashForReporting(clusterSecret, "test")
	if hashedAppName == sql.FailedHashedValue {
		t.Fatalf("expected hashedAppName to not be 'unknown'")
	}

	testData := []struct {
		query         string
		expectedCount int64
	}{
		{`SELECT _`, 8},
		{`SELECT _, _`, 10},
		{`SELECT _, _, _`, 100},
		{`SHOW application_name`, 10000},
	}

	last := r.LastRequestData()
	for _, test := range testData {
		found := false
		for _, s := range last.SqlStats {
			if s.Key.App == hashedAppName && s.Key.Query == test.query {
				if s.Stats.Count != test.expectedCount {
					t.Errorf("quantization incorrect for query %q: expected %d, got %d",
						test.query, test.expectedCount, s.Stats.Count)
				}
				found = true
				break
			}
		}
		if !found {
			t.Errorf("query %q missing from stats", test.query)
		}
	}
}

// TestUpgradeHappensAfterMigration is a regression test to ensure that
// migrations run prior to attempting to upgrade the cluster to the current
// version.
func TestUpgradeHappensAfterMigrations(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.TestingBinaryMinSupportedVersion,
		false, /* initializeVersion */
	)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings: st,
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				BinaryVersionOverride: clusterversion.TestingBinaryMinSupportedVersion,
			},
			SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
				AfterEnsureMigrations: func() {
					// Try to encourage other goroutines to run.
					const N = 100
					for i := 0; i < N; i++ {
						runtime.Gosched()
					}
					require.True(t, st.Version.ActiveVersion(ctx).Less(clusterversion.TestingBinaryVersion))
				},
			},
		},
	})
	s.Stopper().Stop(context.Background())
}
