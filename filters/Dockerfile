# Dependencies stage
FROM golang:1.24-alpine AS deps
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

# Build stage
FROM golang:1.24-alpine AS builder
ARG FILTER=filter_after_2000
WORKDIR /app
COPY --from=deps /app/go.mod /app/go.sum ./
COPY ./common ./common
# RUN mkdir -p ./filter/filter-logic
COPY ./filters/main.go ./filters/main.go
COPY ./filters/filters-library/${FILTER}.go ./filters/filters-library/${FILTER}.go
RUN tree .
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/run_worker ./filters/main.go

# Image final
FROM busybox:latest
COPY --from=builder /app/bin/run_worker /
ENTRYPOINT ["./run_worker"]
