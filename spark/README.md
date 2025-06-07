# Spark service 
## Start the service for the first time
NOTE: If you already create the network while starting an another service, you can skip the first step
1. Create the custom-network network
```
make network
```
2. Build the image 
```
make build
```
3. Run docker compose
```
make up
```

## Restart the service
```
make restart	
```
## Stop the service without deleting containers
```
make stop
```
## Stop the service and delete containers
```
make down

```

## Submit code to Spark
To submit code to Spark Cluster, follow these steps:
1. Determine which packages are needed for the code. For example, if we use Kafka, we will need to include org.apache.spark:spark-sql-kafka-0-10_<SCALA_VERSION>:<SPARK_VERSION>
2. Submit the code to Spark Master:
```
docker exec -it <spark-master-container> spark-submit --packages <needed packages> scripts/<file_name>.py
```
For example, if you use the default configurations and want to submit consumer_spark_minio.py:
```
docker exec -it dbms-spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,io.delta:delta-core_2.12:2.0.0 scripts/consumer_spark_minio.py
```

## Use Spark Notebook
To use Spark Notebook, follow these steps:
1. Access Spark Notebook UI via localhost:8888
2. Get the token:
- Read dbms-spark-notebook token by docker logs dbms-spark-notebook
- Fill the Token to the required field in the UI 
3. Run the notebooks. 