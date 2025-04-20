FROM golang:1.24-alpine AS builder

RUN mkdir -p /build
WORKDIR /build/

COPY ./common ./common

COPY go.mod go.sum ./

COPY ./filters/filter_only_one_country ./filters/filter_only_one_country

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/filter_only_one_country ./filters/filter_only_one_country/main

FROM busybox:latest
COPY --from=builder /build/bin/filter_only_one_country /filter_only_one_country
COPY ./filters/filter_only_one_country/main/config.yaml /config.yaml

ENTRYPOINT ["./filter_only_one_country"]