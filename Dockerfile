# Build Stage
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o broker ./cmd/broker
RUN CGO_ENABLED=0 GOOS=linux go build -o client ./cmd/client
RUN CGO_ENABLED=0 GOOS=linux go build -o api-gateway ./cmd/api-gateway

# Run Stage
FROM alpine:latest
RUN apk --no-cache add ca-certificates curl
WORKDIR /root/
COPY --from=builder /app/broker .
COPY --from=builder /app/client .
COPY --from=builder /app/api-gateway .
COPY --from=builder /app/configs/server.yaml ./configs/server.yaml
EXPOSE 9092 8080
ENTRYPOINT ["./broker"]
