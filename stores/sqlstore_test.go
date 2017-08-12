// Copyright 2017 Apcera Inc. All rights reserved.

package stores

import (
	"testing"
)

var (
	testSQLDriver = "sqlite3"
	testSQLSource = "file::memory:?mode=memory&cache=shared"
)

func createDefaultSQLStore(t *testing.T) Store {
	limits := testDefaultStoreLimits
	ss, err := NewSQLStore(testLogger, testSQLDriver, testSQLSource, &limits)
	if err != nil {
		stackFatalf(t, "Unexpected error: %v", err)
	}
	return ss
}

func cleanupSQLDatastore(t *testing.T) {
}
