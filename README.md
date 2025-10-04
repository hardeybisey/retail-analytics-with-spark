# Retail Analytics with Spark

This project demonstrates building a scalable e-commerce analytics pipeline with Apache Spark, transforming raw data into analytics-ready datasets. This project follows the dimensional models and business rules documented in the [Retail Analytics with Dbt](https://github.com/hardeybisey/retail-analytics-with-dbt) project, providing a hands-on guide to end-to-end data/analytics engineering at scale.


For other stacks, check the following projects:
- [Retail Analytics with Dbt](https://github.com/hardeybisey/retail-analytics-with-dbt)
- [Retail Analytics with Dataflow](https://github.com/hardeybisey/retail-analytics-with-dataflow)
- [Retail Analytics with SQLMesh](https://github.com/hardeybisey/retail-analytics-with-sqlmesh)

---

## Technology Stack
* [Docker](https://docs.docker.com/engine/install/)
* [Spark](https://spark.apache.org/)
* [Minio](https://www.min.io/)
* [Iceberg](https://iceberg.apache.org/)
* [Airflow](https://airflow.apache.org/)
* [Terraform](https://airflow.apache.org/)
* [AWS Glue](https://airflow.apache.org/)

---
## Architecture Overview
[Image]

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
docker network create shared-network

# start spark services
docker compose -f docker-compose.spark.yml up -d

# start airflow services
docker compose -f docker-compose.airflow.yml up -d
```

# navigate to
``` bash
localhost: spark master ui
locahost: spark history server
localhost: airflow ui
localhost: celery flower
```

# cleanup airflow and spark services
docker compose -f docker-compose.airflow.yml down -v
docker compose -f docker-compose.spark.yml down -v
