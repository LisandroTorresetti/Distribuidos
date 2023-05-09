FROM golang:latest

WORKDIR /eofmanager
COPY ../go.mod .
COPY ../go.sum .
COPY ../eofmanager/*.go .
COPY ../utils/ ./utils/
COPY ../eofmanager/config/ ./config/
COPY ../communication/ ./communication/

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o main
ENTRYPOINT ["/bin/sh"]
