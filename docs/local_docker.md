## Architecture Overview
![](../Untitled-2025-08-21-1526.svg)

---
## Project Setup Instructions

### Set up your environment
```bash
# 1. clone the repo
git clone https://github.com/hardeybisey/retail-analytics-with-dbt.git

# 2. Change to the project directory
cd retail-analytics-with-dbt

# 3. Rename the environment file
mv example.env .env

# 4. Edit the .env file to include your Postgres credentials:
PG_USER=<your_postgres_username>
PG_PASSWORD=<your_postgres_password>

# 5. Start Services with Docker
docker compose up -d

# 6. Check that the container is up, you should look for a container named `dbt`
docker ps

# 7. Open a shell session into the dbt container
docker exec -it dbt bash
```

### Running the Project
Inside the dbt container, run the following commands in order:
```bash
# create shared docker network between spark and airflow worker
docker network create --subnet=172.22.0.0/16 shared-network

# start spark services
docker compose -f deployments/docker/docker-compose.spark.yml up -d --build

# start airflow services
docker compose -f deployments/docker/docker-compose.airflow.yml up -d --build
```


## Accessing each services from the UI

|        Application        |URL                          |Credentials                         |
|----------------|-------------------------------|-----------------------------|
|Airflow| [http://localhost:28080](http://localhost:8085) | Login credentials is (AIRFLOW_USER and AIRFLOW_PASS) as defined in the `.env.` file |         |
|MinIO| [http://localhost:29001](http://localhost:9001) |  Login credentials is (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY) as defined in the `.env.` file |           |
|Spark (Master) | [http://localhost:28081](http://localhost:8081)|  |         |
|Spark (History Server) | [http://localhost:18080](http://localhost:8081)|  |         |



## cleanup airflow and spark services
```bash
# stop spark services
docker compose -f deployments/docker/docker-compose.spark.yml down -v

# stop airflow services
docker compose -f deployments/docker/docker-compose.airflow.yml down -v
```
