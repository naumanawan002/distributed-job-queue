FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod ./
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/queue-server ./cmd/server

FROM alpine:3.20
RUN apk --no-cache add ca-certificates curl

WORKDIR /root/
COPY --from=builder /bin/queue-server /usr/local/bin/queue-server

EXPOSE 8090
ENTRYPOINT ["/usr/local/bin/queue-server"]
