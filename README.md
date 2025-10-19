# Retail Analytics with Spark

Build a scalable, end-to-end Retail Analytics Pipeline using Apache Spark, orchestrated by Airflow and backed by Iceberg as the data lakehouse table format. This project transforms raw transactional data into dimensional models ready for business intelligence and advanced analytics following the same data models and business rules defined in the [Retail Analytics with Dbt](https://github.com/hardeybisey/retail-analytics-with-dbt) project.


Other stacks in the *Retail Analytics* series:
- [Retail Analytics with Dbt](https://github.com/hardeybisey/retail-analytics-with-dbt)

---

## Project Overview

The project supports three deployment modes for flexibility across development, testing, and production. Each mode uses the same core architecture but with environment-specific components for orchestration, processing, and storage.

1. **Local (Docker)**: For development and debugging. All services (Airflow, Spark, MinIO, Postgres) run via docker-compose.
2. **Local (Kubernetes)**: For near production testing. Uses Helm charts for Airflow, Spark, MinIO deployment on Kubernetes.
3. **Cloud Deployment**: For production ready deployment using managed services (MWAA, AWS Glue, EMR).

---

## Architecture Overview

| Layer            | Local (Docker)           | Local (Kubernetes)     | Cloud                         |
| ---------------- | ------------------------ | ---------------------- | ----------------------------- |
| Object Storage   | MinIO                    | MinIO                  | Amazon S3                     |
| Orchestration    | Airflow (Docker Compose) | Airflow (Helm on Kind) | MWAA (Managed Airflow)        |
| Data Processing  | Spark (Standalone)       | Spark (on K8s)         | AWS Glue (Spark runtime)      |
| Data Lake Format | Iceberg (MinIO-backed)   | Iceberg (MinIO-backed) | Iceberg (S3-backed)           |
| Metadata DB      | PostgreSQL (Docker)      | PostgreSQL (Pod)       | RDS                           |
| Monitoring       | Local Logs               | Prometheus / Grafana   | Cloud-native Observability    |

---

## Technology Stack

* Apache Airflow
* Apache Spark
* Apache Iceberg
* MinIO
* Docker

---

### Integration Flow

The pipeline is designed around a modular, reproducible flow:

```
        ┌──────────────────┐
        │   Apache Airflow │
        │ (Orchestration)  │
        └──────┬───────────┘
               │ triggers
               ▼
        ┌──────────────────┐
        │   Apache Spark   │
        │ (Processing)     │
        └──────┬───────────┘
               │ reads/writes
               ▼
        ┌──────────────────┐
        │   Apache Iceberg │
        │ (Table Format)   │
        └──────┬───────────┘
               │ stores on
               ▼
        ┌──────────────────┐
        │   MinIO / S3     │
        │ (Object Storage) │
        └──────────────────┘
```

This architecture separates **orchestration**, **processing**, and **storage** concerns, enabling scalability and clear fault boundaries. This modular design enables each layer (orchestration, processing, storage) to evolve independently — for example, switching from MinIO to S3 or scaling Spark from standalone to cluster mode without changing business logic.

## Repository Structure
```
.
├── airflow
│   ├── config
│   ├── dags
│   │   └──
│   └── logs
├── conf
├── deployments
│   ├── aws
│   │   └── terraform
│   ├── docker
│   └── kubernetes
├── docs
├── images
├── jars
├── notebooks
├── raw_input
└── src
    ├── spark_jobs
    │   └──
    └── test
```
--

## Getting Started

### Prerequisites

Before you begin, ensure you have the following tools installed based on your target environment:

- **Docker & Docker Compose** – for running the local development environment.
- **Kubernetes CLI & Helm** – for deploying on Kind or Minikube.
- **AWS CLI & Terraform** – for provisioning and deploying to the cloud.


### Deployment Guides

Each deployment mode has its own setup and configuration guide:

1. [Local (Docker)](docs/local_docker.md) - easiest to set up and ideal for local development and debugging.
2. [Local (Kubernetes)](docs/local_kubernetes.md) - Emulates production using Kind/Minikube with Helm charts.
3. [Cloud](docs/aws_cloud.md) - Full production deployment using AWS managed services.

---

## References

* [Apache Airflow Docs](https://airflow.apache.org/docs/apache-airflow/stable/concepts.html)
* [Apache Spark Docs](https://spark.apache.org/docs/latest/)
* [Apache Iceberg Docs](https://iceberg.apache.org/docs/latest/)
* [MinIO Docs](https://www.min.io/)
* [Docker Docs](https://docs.docker.com/engine/install/)
