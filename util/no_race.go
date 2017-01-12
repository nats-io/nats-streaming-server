// Copyright 2016 Apcera Inc. All rights reserved.

// +build !race

package util

// RaceEnabled indicates that program/tests are running with race detection
// enabled or not. Some tests may chose to skip execution when race
// detection is on.
const RaceEnabled = false
