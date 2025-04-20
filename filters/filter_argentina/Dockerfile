FROM golang:1.24-alpine AS builder

RUN mkdir -p /build
WORKDIR /build/

COPY ./common ./common

COPY go.mod go.sum ./

COPY ./filters/filter_argentina ./filters/filter_argentina

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/filter_argentina ./filters/filter_argentina/main

FROM busybox:latest
COPY --from=builder /build/bin/filter_argentina /filter_argentina
COPY ./filters/filter_argentina/main/config.yaml /config.yaml

ENTRYPOINT ["./filter_argentina"]