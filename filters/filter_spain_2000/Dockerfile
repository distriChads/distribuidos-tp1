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
COPY ./filters/filter_spain_2000 ./filters/filter_spain_2000
COPY ./filters/common_filter ./filters/common_filter
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/run_worker ./filters/filter_spain_2000/main/main.go

# Image final
FROM busybox:latest
COPY --from=builder /app/bin/run_worker /
ENTRYPOINT ["./run_worker"]
