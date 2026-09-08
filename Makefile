.PHONY: build-all build-ingestion build-processing build-notification test-all up down ps

build-all: build-ingestion build-processing build-notification

build-ingestion:
	docker build -t event-platform/ingestion:latest -f services/ingestion/Dockerfile .

build-processing:
	docker build -t event-platform/processing:latest -f services/processing/Dockerfile .

build-notification:
	docker build -t event-platform/notification:latest -f services/notification/Dockerfile .

test-all:
	go test -v ./services/notification/...
	go test -v ./services/processing/...
	go test -v ./services/ingestion/...

up:
	docker-compose -f docker-compose.yml -f docker-compose.local.yml up -d

down:
	docker-compose -f docker-compose.yml -f docker-compose.local.yml down

ps:
	docker-compose -f docker-compose.yml -f docker-compose.local.yml ps
