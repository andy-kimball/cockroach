// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Code generated by generate-logictest, DO NOT EDIT.

package testcockroach_go_testserver_241

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/logictest"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

const configIdx = 21

var logicTestDir string

func init() {
	if bazel.BuiltWithBazel() {
		var err error
		logicTestDir, err = bazel.Runfile("pkg/sql/logictest/testdata/logic_test")
		if err != nil {
			panic(err)
		}
	} else {
		logicTestDir = "../../../../sql/logictest/testdata/logic_test"
	}
}

func TestMain(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)

	defer serverutils.TestingSetDefaultTenantSelectionOverride(
		base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(76378),
	)()

	os.Exit(m.Run())
}

func runLogicTest(t *testing.T, file string) {
	skip.UnderDeadlock(t, "times out and/or hangs")
	logictest.RunLogicTest(t, logictest.TestServerArgs{}, configIdx, filepath.Join(logicTestDir, file))
}

// TestLogic_tmp runs any tests that are prefixed with "_", in which a dedicated
// test is not generated for. This allows developers to create and run temporary
// test files that are not checked into the repository, without repeatedly
// regenerating and reverting changes to this file, generated_test.go.
//
// TODO(mgartner): Add file filtering so that individual files can be run,
// instead of all files with the "_" prefix.
func TestLogic_tmp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var glob string
	glob = filepath.Join(logicTestDir, "_*")
	logictest.RunLogicTests(t, logictest.TestServerArgs{}, configIdx, glob)
}

func TestLogic_cross_version_tenant_backup(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runLogicTest(t, "cross_version_tenant_backup")
}

func TestLogic_mixed_version_bootstrap_tenant(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runLogicTest(t, "mixed_version_bootstrap_tenant")
}

func TestLogic_mixed_version_can_login(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runLogicTest(t, "mixed_version_can_login")
}

func TestLogic_mixed_version_upgrade_preserve_ttl(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runLogicTest(t, "mixed_version_upgrade_preserve_ttl")
}

func TestLogic_upgrade_skip_version(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runLogicTest(t, "upgrade_skip_version")
}
