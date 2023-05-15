FROM golang:latest

WORKDIR /client
COPY ../go.mod .
COPY ../go.sum .
COPY ../datasets/test/ ./datasets/test/
COPY ../datasets/tomitest/ ./datasets/tomitest/
COPY ../socket/ ./socket/
COPY ../utils/ ./utils/
COPY /client/ .

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o main
ENTRYPOINT ["/bin/sh"]