FROM golang:1.24-alpine AS builder

RUN mkdir -p /build
WORKDIR /build/

COPY ./common ./common

COPY go.mod go.sum ./

COPY ./joins/join_movie_credits ./joins/join_movie_credits

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/join_movie_credits ./joins/join_movie_credits/main

FROM busybox:latest
COPY --from=builder /build/bin/join_movie_credits /join_movie_credits
COPY ./joins/join_movie_credits/main/config.yaml /config.yaml

ENTRYPOINT ["./join_movie_credits"]