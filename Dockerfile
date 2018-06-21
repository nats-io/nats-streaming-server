FROM golang:1.10.3

MAINTAINER Ivan Kozlovic <ivan@synadia.com>

COPY . /go/src/github.com/nats-io/nats-streaming-server
WORKDIR /go/src/github.com/nats-io/nats-streaming-server

RUN CGO_ENABLED=0 GOOS=linux   GOARCH=amd64         go build -v -a -tags netgo -installsuffix netgo -ldflags "-s -w -X github.com/nats-io/nats-streaming-server/version.GITCOMMIT=`git rev-parse --short HEAD`" -o pkg/linux-amd64/nats-streaming-server
RUN CGO_ENABLED=0 GOOS=linux   GOARCH=arm   GOARM=6 go build -v -a -tags netgo -installsuffix netgo -ldflags "-s -w -X github.com/nats-io/nats-streaming-server/version.GITCOMMIT=`git rev-parse --short HEAD`" -o pkg/linux-arm6/nats-streaming-server
RUN CGO_ENABLED=0 GOOS=linux   GOARCH=arm   GOARM=7 go build -v -a -tags netgo -installsuffix netgo -ldflags "-s -w -X github.com/nats-io/nats-streaming-server/version.GITCOMMIT=`git rev-parse --short HEAD`" -o pkg/linux-arm7/nats-streaming-server
RUN CGO_ENABLED=0 GOOS=linux   GOARCH=arm64         go build -v -a -tags netgo -installsuffix netgo -ldflags "-s -w -X github.com/nats-io/nats-streaming-server/version.GITCOMMIT=`git rev-parse --short HEAD`" -o pkg/linux-arm64/nats-streaming-server
RUN CGO_ENABLED=0 GOOS=windows GOARCH=amd64         go build -v -a -tags netgo -installsuffix netgo -ldflags "-s -w -X github.com/nats-io/nats-streaming-server/version.GITCOMMIT=`git rev-parse --short HEAD`" -o pkg/win-amd64/nats-streaming-server.exe

ENTRYPOINT ["go"]
CMD ["version"]
