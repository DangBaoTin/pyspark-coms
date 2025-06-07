# MinIO service 
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

## Create MinIO bucket
Some test codes require a MinIO bucket to work. In order to create the MinIO bucket, follow these steps:
1. Access MinIO UI via localhost:9001

2. Input the username and password. The default username and password are minio_user and minio_password, respectively. The username and password can be changed via editing docker-compose.yml file. 

3. Access "Buckets" in Administrators section. 

4. Click "Create Bucket" and fill the bucket name as well as some other configurations.

## Create MinIO Access Keys
NOTE: for Spark, it will accept username and password as Access Key, so we do not need to create the access key
Some test codes require a MinIO Access Keys to work. In order to create the MinIO bucket, follow these steps:
1. Access MinIO UI via localhost:9001

2. Input the username and password. The default username and password are minio_user and minio_password, respectively. The username and password can be changed via editing docker-compose.yml file. 

3. Access "Access Keys" in User section. 

4. Click "Create Access Key" and fill in some configurations for the key. 

5. Record the Access Key and the Secret Key, as we will not be able to view them again after creation. 