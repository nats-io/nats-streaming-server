// Copyright 2013-2016 Apcera Inc. All rights reserved.

package graft

import (
	"errors"
)

var (
	ClusterNameErr  = errors.New("graft: Cluster name can not be empty")
	ClusterSizeErr  = errors.New("graft: Cluster size can not be 0")
	HandlerReqErr   = errors.New("graft: Handler is required")
	RpcDriverReqErr = errors.New("graft: RPCDriver is required")
	LogReqErr       = errors.New("graft: Log is required")
	LogNoExistErr   = errors.New("graft: Log file does not exist")
	LogNoStateErr   = errors.New("graft: Log file does not have any state")
	LogCorruptErr   = errors.New("graft: Encountered corrupt log file")
	NotImplErr      = errors.New("graft: Not implemented")
)
