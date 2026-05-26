FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o subscriber .

FROM alpine:latest
COPY --from=builder /app/subscriber /subscriber
CMD ["/subscriber"]
