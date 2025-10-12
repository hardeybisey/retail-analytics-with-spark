## Architecture Overview
![](Untitled-2025-08-21-1526.svg)


## Architecture Overview
![](Untitled-2025-08-21-1526.svg)

---
## Key Features
- **Scalable Big Data Processing Platform**
- **Modular and Maintainable Architecture**
- **Automated Data Pipelines**
- **Monitoring and Logging**
- **CI/CD Automation**
- **Production-Ready Setup**


## Best Practices
- **Infrastructure as Code (IaC):**
- **Modular Code Structure:**
- **Environment Isolation:**

### Local Setup Instructions

## Prerequisites
* Docker
* Docker Compose

## Environment Setup
1. clone repo
2. cd into repo
3. create a .venv file
4. update environment variables

## start services
```bash
# create shared docker network between spark and airflow worker
docker network create --subnet=172.22.0.0/16 shared-network

# start spark services
docker compose -f docker-compose.spark.yml up -d --build

# start airflow services
docker compose -f docker-compose.airflow.yml up -d --build
```

# navigate to
``` bash
localhost: spark master ui
locahost: spark history server
localhost: airflow ui
localhost: celery flower
```

# cleanup airflow and spark services
```bash
docker compose -f docker-compose.airflow.yml down
docker compose -f docker-compose.spark.yml down
```


```bash
# for dev
docker compose -f docker-compose.spark.yml up -d --build && docker compose -f docker-compose.airflow.yml up -d --build
docker compose -f docker-compose.airflow.yml down -v && docker compose -f docker-compose.spark.yml down -v
```

```bash
# for dev
docker compose -f docker-compose.spark.yml up -d  && docker compose -f docker-compose.airflow.yml up -d
docker compose -f docker-compose.airflow.yml down && docker compose -f docker-compose.spark.yml down
```



--master $SPARK_MASTER_URL

spark-submit --executor-memory 1G \
     --executor-cores 1 \
     --total-executor-cores 1  \
     /opt/airflow/scripts/test.py

spark-submit --executor-memory 1G \
     --executor-cores 1 \
     --total-executor-cores 1  \
     /opt/airflow/scripts/migrations_v1.py


airflow url: http://localhost:8080/
spark master url: http://localhost:8081/
spark worker-1 url: http://localhost:8082/
spark worker-2 url: http://localhost:8083/
spark history-server: http://localhost:18080/
MinIO s3 url: http://localhost:9001/

<!--
df = spark.createDataFrame(data, schema)
# Add a temporary column with monotonically increasing id
df = df.withColumn("temp_id", monotonically_increasing_id())
# Define a window specification
window_spec = Window.orderBy("temp_id")
# Add a surrogate key with consecutive values using row_number()
df = df.withColumn("ID", row_number().over(window_spec)).drop("temp_id") -->
