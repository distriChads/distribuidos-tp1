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
COPY ./master_group_by/master_group_by_country_sum ./master_group_by/master_group_by_country_sum
COPY ./group_by/common_group_by ./group_by/common_group_by
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/run_worker ./master_group_by/master_group_by_country_sum/main/main.go

# Image final
FROM busybox:latest
COPY --from=builder /app/bin/run_worker /
ENTRYPOINT ["./run_worker"]
