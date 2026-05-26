FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY . .
RUN go mod tidy && go build -o publisher .

FROM alpine:latest
COPY --from=builder /app/publisher /publisher
CMD ["/publisher"]
