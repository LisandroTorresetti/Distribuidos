SHELL := /bin/bash
PWD := $(shell pwd)

build-images:
	docker build -f ./server/Dockerfile -t "server:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
.PHONY: build-images

mandale-mecha: build-images
	docker-compose -f docker-compose.yaml up -d --build
.PHONY: mandale-mecha

stop-app:
	docker-compose -f docker-compose.yaml stop -t 1
.PHONY: stop-app

delete-app: stop-app
	docker-compose -f docker-compose.yaml down
.PHONY: delete-app


logs:
	docker-compose -f docker-compose.yaml logs -f
.PHONY: logs