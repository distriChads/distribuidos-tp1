FROM golang:1.24-alpine AS builder

RUN mkdir -p /build
WORKDIR /build/

COPY ./common ./common

COPY go.mod go.sum ./

COPY ./topn/top_five_country_budget ./topn/top_five_country_budget

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/top_five_country_budget ./topn/top_five_country_budget/main

FROM busybox:latest
COPY --from=builder /build/bin/top_five_country_budget /top_five_country_budget
COPY ./topn/top_five_country_budget/main/config.yaml /config.yaml

ENTRYPOINT ["./top_five_country_budget"]