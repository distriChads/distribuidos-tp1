FROM golang:1.24-alpine AS builder

RUN mkdir -p /build
WORKDIR /build/

COPY ./common ./common

COPY go.mod go.sum ./

COPY ./filters/filter_after_2000 ./filters/filter_after_2000

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/filter_after_2000 ./filters/filter_after_2000/main

FROM busybox:latest
COPY --from=builder /build/bin/filter_after_2000 /filter_after_2000
COPY ./filters/filter_after_2000/main/config.yaml /config.yaml

ENTRYPOINT ["./filter_after_2000"]