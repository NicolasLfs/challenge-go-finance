FROM golang:1.26.1-alpine

WORKDIR /workspace
COPY go.mod ./
COPY . .

RUN go test ./...

CMD ["go", "test", "./..."]