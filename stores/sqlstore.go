// Copyright 2017 Apcera Inc. All rights reserved.

package stores

import (
	"github.com/nats-io/nats-streaming-server/logger"
)

// SQLStore is a factory for message and subscription stores backed by
// a SQL database.
type SQLStore struct {
	Store
}

// NewSQLStore returns a factory for stores held in memory.
// If not limits are provided, the store will be created with
// DefaultStoreLimits.
func NewSQLStore(log logger.Logger, driver, source string, limits *StoreLimits) (Store, error) {
	ms, err := NewMemoryStore(log, limits)
	if err != nil {
		return nil, err
	}
	s := &SQLStore{Store: ms}
	return s, nil
}

func (s *SQLStore) Name() string {
	return TypeSQL
}
