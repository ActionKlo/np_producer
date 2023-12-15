FROM golang:1.20-alpine
LABEL authors="andreisarnouski"

WORKDIR /app

COPY . .

RUN go mod download

COPY .env cmd/producer/
COPY .env cmd/consumer/

WORKDIR /app/cmd/producer

RUN go build -o app .

CMD ["./app"]