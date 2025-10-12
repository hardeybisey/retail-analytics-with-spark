## Architecture Overview
![](../Untitled-2025-08-21-1526.svg)


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
cd retail-analytics-with-spark/deployments/docker
docker compose -f docker-compose.spark.yml up -d --build && docker compose -f docker-compose.airflow.yml up -d --build
docker compose -f docker-compose.airflow.yml down -v && docker compose -f docker-compose.spark.yml down -v
```

```bash
# for dev
cd retail-analytics-with-spark/deployments/docker
docker compose -f docker-compose.spark.yml up -d  && docker compose -f docker-compose.airflow.yml up -d
docker compose -f docker-compose.airflow.yml down && docker compose -f docker-compose.spark.yml down
```
