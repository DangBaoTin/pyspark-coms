network:
	docker network create coms-network || true

build:
	@make down
	docker compose -f docker-compose.yml build
	@make up

up:
	docker compose -f docker-compose.yml up -d 

down:
	docker compose -f docker-compose.yml down 

stop:
	docker compose -f docker-compose.yml stop

# Set up and start everything for the first time
first:
	@make network
	@make build
	@make up
	@echo "Airflow UI is available at http://localhost:8081"
	
restart:
	@make down
	@make up