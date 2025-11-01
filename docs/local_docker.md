## Architecture Overview
![](../images/local_docker.svg)


---
## Project Setup Instructions

## **1. Environment Setup**

```bash
# 1. Clone the repository
git clone https://github.com/hardeybisey/retail-analytics-with-spark.git

# 2. Navigate into the project directory
cd retail-analytics-with-spark

# 3. Configure environment variables
# Rename the example environment file located in `deployments/docker`
mv deployments/docker/example.env deployments/docker/.env

# (Optional) Update the `.env` file if you wish to override default configurations.
```

---

## **2. Running the Project**

### **Step 1. Create a shared Docker network**

This allows Spark and Airflow containers to communicate.

```bash
docker network create shared-network
```

### **Step 2. Start Spark services**

> Note: This may take several minutes during the first run as images are pulled and initialised.

```bash
docker compose -f deployments/docker/docker-compose.spark.yml up -d
```

### **Step 3. Start Airflow services**

> Similarly, this may take a while during the initial setup.

```bash
docker compose -f deployments/docker/docker-compose.airflow.yml up -d
```


### **Step 4. Verify that all containers are running**

```bash
docker ps
```

You should see entries for the Spark master, Spark worker(s), Airflow webserver, scheduler, and worker containers.

---

## **3. Accessing Services via Web UI**

| **Application**          | **URL**                                          | **Credentials**                                                 |
| ------------------------ | ------------------------------------------------ | --------------------------------------------------------------- |
| **Airflow**              | [http://localhost:28080](http://localhost:28080) | Use `AIRFLOW_USER` and `AIRFLOW_PASS` from `.env`               |
| **MinIO**                | [http://localhost:29001](http://localhost:29001) | Use `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` from `.env` |
| **Spark Master**      | [http://localhost:28081](http://localhost:28081) | –                                                               |
| **Spark History Server** | [http://localhost:18080](http://localhost:18080) | –                                                               |

## Airflow
![](../images/airflow.png)

## Spark Master
![](../images/spark_master.png)


## MinIO
![](../images/minio.png)

## Spark History Server
![](../images/history_server.png)

---

## **4. Stopping and Cleaning Up**

To gracefully stop and remove the running containers (including their volumes):

```bash
# Stop and remove Spark services
docker compose -f deployments/docker/docker-compose.spark.yml down -v

# Stop and remove Airflow services
docker compose -f deployments/docker/docker-compose.airflow.yml down -v
```
