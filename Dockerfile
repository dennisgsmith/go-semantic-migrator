ARG GO_VERSION=1.23

FROM golang:${GO_VERSION}-alpine

RUN apk add --no-cache ca-certificates

ARG GOARCH=amd64
ENV CGO_ENABLED=0

COPY go.* ./
RUN go mod download

COPY . .

ONBUILD VOLUME ["/schema"]
ONBUILD VOLUME ["/repeat"]
ONBUILD VOLUME ["/seed"]

ONBUILD RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	GOOS=linux GOARCH=${GOARCH} \
	go build -ldflags="-w -s" -o /go/bin/migrate
