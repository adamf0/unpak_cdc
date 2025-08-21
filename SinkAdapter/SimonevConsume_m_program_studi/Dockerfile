# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Install git jika perlu dependen
RUN apk add --no-cache git

# Copy go.mod dan go.sum untuk caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build binary
RUN go build -o consumer main.go

# Final stage
FROM alpine:latest

WORKDIR /app

# Copy binary
COPY --from=builder /app/consumer .

# Set timezone jika perlu
ENV TZ=Asia/Jakarta

# Run
CMD ["./consumer"]
