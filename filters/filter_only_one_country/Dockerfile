FROM golang:1.24-alpine

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

WORKDIR /app/filters/filter_only_one_country
RUN go build -o filter_only_one_country

CMD ["./filter_only_one_country"] 