FROM golang:latest

WORKDIR /joiners
COPY ../go.mod .
COPY ../go.sum .
COPY ../utils/ ./utils/
COPY ../communication/ ./communication/
COPY ../domain/entities/ ./domain/entities/
COPY ../domain/business/ ./domain/business/
COPY ../joiners/factory/ ./joiners/factory/
COPY ../joiners/*.go .

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o main
ENTRYPOINT ["/bin/sh"]