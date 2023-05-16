SHELL := /bin/bash
PWD := $(shell pwd)

build-images:
	docker build -f ./server/Dockerfile -t "server:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
	docker build -f ./workers/Dockerfile -t "workers:latest" .
	docker build -f ./joiners/Dockerfile -t "joiners:latest" .
	docker build -f ./queryhandlers/Dockerfile -t "queryhandlers:latest" .
	docker build -f ./eofmanager/Dockerfile -t "eofmanager:latest" .
.PHONY: build-images

mandale-mecha:
	docker-compose -f docker-compose.yaml up -d --build
.PHONY: mandale-mecha

stop-app:
	docker-compose -f docker-compose.yaml stop -t 1
.PHONY: stop-app

delete-app: stop-app
	docker-compose -f docker-compose.yaml down
	echo "TE ESTAS PORTANDO MAL SERAS CASTIGADO"
.PHONY: delete-app

logs:
	docker-compose -f docker-compose.yaml logs -f
.PHONY: logs