FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o server cmd/server/main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/server .
COPY --from=builder /app/web ./web

EXPOSE 8080
ENTRYPOINT ["./server"]
