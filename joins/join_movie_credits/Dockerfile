# Dependencies stage
FROM golang:1.24-alpine AS deps
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

# Build stage
FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY --from=deps /app/go.mod /app/go.sum ./
COPY ./common ./common
COPY ./joins/join_movie_credits ./joins/join_movie_credits
COPY ./joins/common_join ./joins/common_join
COPY ./common_statefull_worker ./common_statefull_worker
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/run_worker ./joins/join_movie_credits/main/main.go

# Image final
FROM busybox:latest
COPY --from=builder /app/bin/run_worker /
ENTRYPOINT ["./run_worker"]
