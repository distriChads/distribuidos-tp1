FROM golang:1.24-alpine AS builder

RUN mkdir -p /build
WORKDIR /build/

COPY ./common ./common

COPY go.mod go.sum ./

COPY ./group_by/group_by_country_sum ./group_by/group_by_country_sum

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/group_by_country_sum ./group_by/group_by_country_sum/main

FROM busybox:latest
COPY --from=builder /build/bin/group_by_country_sum /group_by_country_sum
COPY ./group_by/group_by_country_sum/main/config.yaml /config.yaml

ENTRYPOINT ["./group_by_country_sum"]