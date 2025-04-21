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
COPY ./filters/filter_after_2000 ./filters/filter_after_2000
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/filter_runner ./filters/filter_after_2000/main/main.go

# Image final
FROM busybox:latest
COPY --from=builder /app/bin/filter_runner /
ENTRYPOINT ["./filter_runner"]
