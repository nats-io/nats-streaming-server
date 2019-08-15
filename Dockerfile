FROM golang:1.11.13

MAINTAINER Ivan Kozlovic <ivan@synadia.com>

COPY . /go/src/github.com/nats-io/nats-streaming-server
WORKDIR /go/src/github.com/nats-io/nats-streaming-server

# Set the NATS Server git commit based on the rev that
# we actually vendor. This can be found in the
# vendor/manifest file.
ARG NATS_GIT_COMMIT='github.com/nats-io/nats-streaming-server/vendor/github.com/nats-io/nats-server/v2/server.gitCommit=c8ca58e'

RUN CGO_ENABLED=0 GO111MODULE=off GOOS=linux   GOARCH=amd64         go build -v -a -tags netgo -installsuffix netgo -ldflags "-s -w -X github.com/nats-io/nats-streaming-server/server.gitCommit=`git rev-parse --short HEAD` -X ${NATS_GIT_COMMIT}" -o pkg/linux-amd64/nats-streaming-server
RUN CGO_ENABLED=0 GO111MODULE=off GOOS=linux   GOARCH=arm   GOARM=6 go build -v -a -tags netgo -installsuffix netgo -ldflags "-s -w -X github.com/nats-io/nats-streaming-server/server.gitCommit=`git rev-parse --short HEAD` -X ${NATS_GIT_COMMIT}" -o pkg/linux-arm6/nats-streaming-server
RUN CGO_ENABLED=0 GO111MODULE=off GOOS=linux   GOARCH=arm   GOARM=7 go build -v -a -tags netgo -installsuffix netgo -ldflags "-s -w -X github.com/nats-io/nats-streaming-server/server.gitCommit=`git rev-parse --short HEAD` -X ${NATS_GIT_COMMIT}" -o pkg/linux-arm7/nats-streaming-server
RUN CGO_ENABLED=0 GO111MODULE=off GOOS=linux   GOARCH=arm64         go build -v -a -tags netgo -installsuffix netgo -ldflags "-s -w -X github.com/nats-io/nats-streaming-server/server.gitCommit=`git rev-parse --short HEAD` -X ${NATS_GIT_COMMIT}" -o pkg/linux-arm64/nats-streaming-server
RUN CGO_ENABLED=0 GO111MODULE=off GOOS=windows GOARCH=amd64         go build -v -a -tags netgo -installsuffix netgo -ldflags "-s -w -X github.com/nats-io/nats-streaming-server/server.gitCommit=`git rev-parse --short HEAD` -X ${NATS_GIT_COMMIT}" -o pkg/win-amd64/nats-streaming-server.exe

ENTRYPOINT ["go"]
CMD ["version"]
