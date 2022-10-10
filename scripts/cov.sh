#!/bin/bash
# Run from directory above via ./scripts/cov.sh

check_file () {
    # If the content of the file is simply "mode: atomic", then it means that the
    # code coverage did not complete due to a panic in one of the tests.
    if [[ $(cat ./cov/$2) == "mode: atomic" ]]; then
        echo "#############################################"
        echo "## Code coverage for $1 package failed ##"
        echo "#############################################"
        exit 1
    fi
}

# Do not globally set the -e flag because we don't a flapper to prevent the push to coverall.

export GO111MODULE="on"

go install github.com/wadey/gocovmerge@latest

rm -rf ./cov
mkdir cov
#
# Since it is difficult to get a full run without a flapper, do not use `-failfast`.
# It is better to have one flapper or two and still get the report than have
# to re-run the whole code coverage. One or two failed tests should not affect
# so much the code coverage.
#
# However, we need to take into account that if there is a panic in one test, all
# other tests in that package will not run, which then would cause the code coverage
# to drastically be lowered. In that case, we don't want the code coverage to be
# uploaded.
#
go test -v -covermode=atomic -coverprofile=./cov/server.out -coverpkg=./server,./stores,./util ./server
check_file "server" "server.out"
go test -v -covermode=atomic -coverprofile=./cov/server2.out -coverpkg=./server,./stores,./util -run=TestPersistent ./server -encrypt
check_file "server" "server2.out"
go test -v -covermode=atomic -coverprofile=./cov/logger.out ./logger
check_file "logger" "logger.out"
go test -v -covermode=atomic -coverprofile=./cov/stores1.out ./stores
check_file "stores" "stores1.out"
go test -v -covermode=atomic -coverprofile=./cov/stores2.out -run=TestCS/FILE ./stores -fs_no_buffer
check_file "stores" "stores2.out"
go test -v -covermode=atomic -coverprofile=./cov/stores3.out -run=TestCS/FILE ./stores -fs_set_fds_limit
check_file "stores" "stores3.out"
go test -v -covermode=atomic -coverprofile=./cov/stores4.out -run=TestCS/FILE ./stores -fs_no_buffer -fs_set_fds_limit
check_file "stores" "stores4.out"
go test -v -covermode=atomic -coverprofile=./cov/stores5.out -run=TestFS ./stores -fs_no_buffer
check_file "stores" "stores5.out"
go test -v -covermode=atomic -coverprofile=./cov/stores6.out -run=TestFS ./stores -fs_set_fds_limit
check_file "stores" "stores6.out"
go test -v -covermode=atomic -coverprofile=./cov/stores7.out -run=TestFS ./stores -fs_no_buffer -fs_set_fds_limit
check_file "stores" "stores7.out"
go test -v -covermode=atomic -coverprofile=./cov/stores8.out -run=TestCS ./stores -encrypt
check_file "stores" "stores8.out"
go test -v -covermode=atomic -coverprofile=./cov/util.out ./util
check_file "util" "util.out"

# At this point, if that fails, we want the caller to know about the failure.
set -e
gocovmerge ./cov/*.out > acc.out
rm -rf ./cov

# If no argument passed, launch a browser to see the results.
if [[ $1 == "" ]]; then
    go tool cover -html=acc.out
fi
set +e
