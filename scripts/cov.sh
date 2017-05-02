#!/bin/bash -e
# Run from directory above via ./scripts/cov.sh

rm -rf ./cov
mkdir cov
go test -v -covermode=count -coverprofile=./cov/server.out -run=Test[^Signal] ./server
go test -v -covermode=count -coverprofile=./cov/server2.out -run=TestSignal ./server
# repeat these server FT tests but focus on stores package
go test -v -covermode=count -coverprofile=./cov/stores1.out -run=TestFTPartition -coverpkg=./stores ./server
go test -v -covermode=count -coverprofile=./cov/stores2.out ./stores
go test -v -covermode=count -coverprofile=./cov/stores3.out -run=TestFS ./stores -no_buffer
go test -v -covermode=count -coverprofile=./cov/stores4.out -run=TestFS ./stores -set_fds_limit
go test -v -covermode=count -coverprofile=./cov/stores5.out -run=TestFS ./stores -no_buffer -set_fds_limit
go test -v -covermode=count -coverprofile=./cov/util.out ./util
gocovmerge ./cov/*.out > acc.out
rm -rf ./cov

# If we have an arg, assume travis run and push to coveralls. Otherwise launch browser results
if [[ -n $1 ]]; then
    $HOME/gopath/bin/goveralls -coverprofile=acc.out
    rm -rf ./acc.out
else
    go tool cover -html=acc.out
fi
