# Starts MinIO, Spark, and Airflow
up-all:
	@echo "--- Starting MinIO ---"
	@cd minio && make up
	@echo "--- Starting Spark Cluster ---"
	@cd spark && make up
	@echo "--- Starting Airflow ---"
	@cd airflow && make up
	@echo "...All services are running!..."
	@echo "Spark UI: http://localhost:8080"
	@echo "Airflow UI: http://localhost:8081"

# Stops all services
down-all:
	@echo "--- Stopping Airflow ---"
	@cd airflow && make down
	@echo "--- Stopping Spark Cluster ---"
	@cd spark && make down
	@echo "--- Stopping MinIO ---"
	@cd minio && make down
	@echo "...All services stopped..."

# Restarts all services
restart-all:
	@make down-all
	@make up-all