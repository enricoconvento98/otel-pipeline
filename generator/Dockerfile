# --- Stage 1: build ---
FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY . .
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/generator .

# --- Stage 2: runtime minimale ---
FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /out/generator /generator
ENTRYPOINT ["/generator"]
