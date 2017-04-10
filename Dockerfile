FROM golang:1.7.5

MAINTAINER Ivan Kozlovic <ivan.kozlovic@apcera.com>

COPY . /go/src/github.com/nats-io/nats-streaming-server
WORKDIR /go/src/github.com/nats-io/nats-streaming-server

RUN CGO_ENABLED=0 go install -v -a -tags netgo -installsuffix netgo -ldflags "-s -w -X github.com/nats-io/nats-streaming-server/version.GITCOMMIT=`git rev-parse --short HEAD`"

EXPOSE 4222 8222
ENTRYPOINT ["nats-streaming-server"]
CMD ["--help"]
