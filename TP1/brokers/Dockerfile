FROM golang:latest

WORKDIR /brokers
COPY ../go.mod .
COPY ../go.sum .
COPY ../utils/ ./utils/
COPY ../domain/communication/ ./domain/communication/
COPY ../domain/entities/ ./domain/entities/
COPY ../brokers/errors ./brokers/errors
COPY ../brokers/station ./brokers/station
COPY ../brokers/trip ./brokers/trip
COPY ../brokers/weather ./brokers/weather
COPY ../brokers/*.go .

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o main
ENTRYPOINT ["/bin/sh"]