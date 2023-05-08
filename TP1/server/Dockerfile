FROM golang:latest

WORKDIR /server
COPY ../go.mod .
COPY ../go.sum .
COPY ../socket/ ./socket/
COPY ../utils/ ./utils/
COPY ../server/handler/ ./server/handler/
COPY ../server/*.go .
COPY ../server/config/ ./config/
COPY ../communication/ ./communication/

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o main
ENTRYPOINT ["/bin/sh"]
