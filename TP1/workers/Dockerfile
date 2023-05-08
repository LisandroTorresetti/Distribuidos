FROM golang:latest

WORKDIR /workers
COPY ../go.mod .
COPY ../go.sum .
COPY ../utils/ ./utils/
COPY ../communication/ ./communication/
COPY ../domain/entities/ ./domain/entities/
COPY ../workers/factory/ ./workers/factory/
COPY ../workers/*.go .

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o main
ENTRYPOINT ["/bin/sh"]