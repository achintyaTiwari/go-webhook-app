FROM golang:1.19-alpine AS builder
WORKDIR /app
COPY go.* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /webhook

FROM alpine:latest  
WORKDIR /app
COPY --from=builder /webhook .
CMD ["./webhook"]