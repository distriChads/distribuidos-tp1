FROM golang:1.24-alpine

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

WORKDIR /app/filters/filter_argentina
RUN go build -o filter_argentina

CMD ["./filter_argentina"] 