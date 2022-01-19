#!/bin/bash -e
# Run from directory above via ./scripts/cov.sh

export GO111MODULE="on"

go get github.com/mattn/goveralls
go get github.com/wadey/gocovmerge

rm -rf ./cov
mkdir cov
go test -v -failfast -covermode=atomic -coverprofile=./cov/server.out -coverpkg=./server,./stores,./util ./server
go test -v -failfast -covermode=atomic -coverprofile=./cov/server2.out -coverpkg=./server,./stores,./util -run=TestPersistent ./server -encrypt
go test -v -failfast -covermode=atomic -coverprofile=./cov/logger.out ./logger
go test -v -failfast -covermode=atomic -coverprofile=./cov/stores1.out ./stores
go test -v -failfast -covermode=atomic -coverprofile=./cov/stores2.out -run=TestCS/FILE ./stores -fs_no_buffer
go test -v -failfast -covermode=atomic -coverprofile=./cov/stores3.out -run=TestCS/FILE ./stores -fs_set_fds_limit
go test -v -failfast -covermode=atomic -coverprofile=./cov/stores4.out -run=TestCS/FILE ./stores -fs_no_buffer -fs_set_fds_limit
go test -v -failfast -covermode=atomic -coverprofile=./cov/stores5.out -run=TestFS ./stores -fs_no_buffer
go test -v -failfast -covermode=atomic -coverprofile=./cov/stores6.out -run=TestFS ./stores -fs_set_fds_limit
go test -v -failfast -covermode=atomic -coverprofile=./cov/stores7.out -run=TestFS ./stores -fs_no_buffer -fs_set_fds_limit
go test -v -failfast -covermode=atomic -coverprofile=./cov/stores8.out -run=TestCS ./stores -encrypt
go test -v -failfast -covermode=atomic -coverprofile=./cov/util.out ./util
gocovmerge ./cov/*.out > acc.out
rm -rf ./cov

# Without argument, launch browser results. We are going to push to coveralls only
# from Travis.yml and after success of the build (and result of pushing will not affect
# build result).
if [[ $1 == "" ]]; then
    go tool cover -html=acc.out
fi
