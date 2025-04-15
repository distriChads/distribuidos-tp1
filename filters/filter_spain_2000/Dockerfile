FROM golang:1.24-alpine

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

WORKDIR /app/filters/filter_spain_2000
RUN go build -o filter_spain_2000

CMD ["./filter_spain_2000"] 