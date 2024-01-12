FROM golang:1.21-alpine
LABEL authors="andreisarnouski"

WORKDIR /app

COPY . .

RUN go env -w GOCACHE=/go-cache
RUN go env -w GOMODCACHE=/gomod-cache

RUN --mount=type=cache,target=/gomod-cache \
    go mod download

COPY .env cmd/producer/
COPY .env cmd/consumer/

WORKDIR /app/cmd/producer

RUN --mount=type=cache,target=/gomod-cache --mount=type=cache,target=/go-cache \
    go build -mod=readonly -o app .

CMD ["./app"]