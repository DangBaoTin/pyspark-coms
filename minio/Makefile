network:
	docker network create coms-network
	
build:
	@make down
	docker compose -f docker-compose.yml build
	@make up
	
up:
	docker compose -f docker-compose.yml up -d 
	
down:
	docker compose -f docker-compose.yml down 

stop:
	docker compose stop

	
first:
	@make network
	@make build
	@make up
	
restart:
	@make down 
	@make up