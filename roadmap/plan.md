# 📊 Retail Analytics Platform – Project Plan

## Overview

This project explores how to build scalable Retail Analytics solutions using six modern data engineering stacks. Each implementation answers the same business questions using different tooling. It serves as a benchmark, learning lab, and portfolio project to compare how each stack supports:

- **Data ingestion**
- **Transformation**
- **Orchestration**
- **Visualisation**
- **CI/CD and DevOps**

---

## 📦 Dataset

**Source:** [Olist Brazilian E-commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data)<br>
**Size:** ~100k Orders (2016–2018)<br>
**Schema:**
![Data Schema](../images/HRhd2Y0.png)


## 🔍 Learning Goals

- Compare DAG-based vs SQL-based transformation tools
- Integrate modern orchestration engines (Airflow, Beam, etc.)
- Build a unified analytics pipeline across multiple stacks
- Evaluate trade-offs in speed, complexity, maintainability
- Practice DevOps workflows (Docker, CI/CD, Terraform, K8s)

---

## 🗂️ Projects & Tech Stacks

### 1. 🧮 Retail Analytics with DBT
**Stack**: `dbt`, `duckdb`, `superset`, `github actions`<br>
**Focus**: Modular SQL modelling and dashboarding on lightweight stack<br>
**Goal**: Rapid prototyping with in-memory compute for local-first analytics<br>

| Phase | Objective | Key Activities |
|-------|-----------|----------------|
| Foundation & Ingestion | Set up lightweight SQL analytics environment | - Configure Docker with DuckDB, dbt, Superset <br> - Ingest Olist CSVs into DuckDB <br> - Test connectivity between services |
| Core Transformation | Build modular, tested SQL models | - Create staging models <br> - Build dimension/fact models <br> - Add tests (`unique`, `not null`) and docs |
| Visualisation | Enable self-service BI | - Connect Superset to dbt models <br> - Build dashboards (e.g. revenue, order fulfilment) |
| CI/CD Automation | Automate testing and deployment | - Create GitHub Actions pipeline <br> - Trigger `dbt build` on PRs <br> - Block bad code merges |

---

### 2. 📊 Retail Analytics with SQLMesh
**Stack**: `sqlmesh`, `postgres`, `looker`, `github actions`<br>
**Focus**: Versioned SQL DAGs with environment promotion<br>
**Goal**: Emulate production workflows and explore dbt alternative<br>


| Phase | Objective | Key Activities |
|-------|-----------|----------------|
| Environment & Load | Initialise SQLMesh on production-grade DB | - Docker setup with Postgres and SQLMesh <br> - Load Olist data to Postgres <br> - Configure SQLMesh project |
| Data Modeling | Build reliable version-controlled data models | - Define models and partitions <br> - Add audits and constraints <br> - Use `sqlmesh plan` for data diffs |
| BI Integration | Enable exploration via Superset | - Connect BI tool to final models <br> - Build LookML or BI dashboards |
| CI/CD & Backfills | Automate deployment and smart reprocessing | - CI pipeline to run `sqlmesh plan` <br> - Apply only changed models <br> - Showcase backfilling efficiency |

---

### 3. ⚙️ Retail Analytics with Airflow
**Stack**: `airflow`, `snowflake`, `kafka`, `debezium`, `github actions`, `great expectation`<br>
**Focus**: Event-driven batch orchestration<br>
**Goal**: Use real-time ingestion and Snowflake for warehouse<br>

| Phase | Objective | Key Activities |
|-------|-----------|----------------|
| Infrastructure Setup | Create robust orchestration and ingestion infra | - Deploy Airflow, Kafka, Debezium <br> - Setup Snowflake and staging tables <br> - Ingest from Kafka into Snowflake |
| Transformation & Orchestration | Build ELT pipelines via DAGs | - Write SQL transformation scripts <br> - Define DAGs for task orchestration <br> - Add data quality checks with great expectation |
| BI Layer | Expose insights via dashboards | - Connect Superset to Snowflake <br> - Build real-time dashboards (orders, sales) |
| Monitoring & CI | Add pipeline robustness | - CI/CD for DAG deployment <br> - Set alerts for failure/SLA misses |

---

### 4. 🔥 Retail Analytics with Spark
**Stack**: `spark`, `s3`, `glue`, `emr`, `iceberg`,  `redshift`, `devops`<br>
**Focus**: Distributed processing and cloud-native analytics<br>
**Goal**: Handle large-scale transformation and cross-service orchestration<br>

| Phase | Objective | Key Activities |
|-------|-----------|----------------|
| AWS Infra Setup | Provision scalable cloud infra | - Use S3 as data lake <br> - Setup Glue catalog, EMR, Redshift |
| Data Processing | Transform large data with Spark | - Load raw data to Parquet <br> - Create dimensional models <br> - Partition data for performance |
| Analytics Layer | Serve insights with Redshift | - Query via Redshift or Spectrum <br> - Build BI dashboards <br> - Optimise Redshift performance |
| DevOps & Automation | Automate infra and data pipeline | - Use Terraform/IaC for infra <br> - Use Step Functions or Airflow for orchestration <br> - CI pipeline to deploy Spark jobs |

---

### 5. 📡 Retail Analytics with Dataflow
**Stack**: `apache beam`, `bigquery`, `pub/sub`, `gcp devops`<br>
**Focus**: Stream and batch processing in one codebase<br>
**Goal**: Unify batch and real-time dataflow for analytics pipelines<br>

| Phase | Objective | Key Activities |
|-------|-----------|----------------|
| GCP Setup & Ingestion | Build a unified batch/streaming pipeline | - Setup GCP project and IAM roles <br> - Ingest data via Pub/Sub for streaming and Cloud Storage for batch |
| Beam Pipelines | Build resilient pipelines with Apache Beam | - Develop Beam jobs for both batch and stream ingestion <br> - Transform data using Beam’s `DoFn`, `ParDo`, and `GroupByKey` |
| BigQuery Warehousing | Model for analytics in BigQuery | - Write SQL transformations in BQ <br> - Build fact/dimension tables from Beam output |
| DevOps & Monitoring | Automate deployment and monitor pipeline health | - Use Terraform for provisioning (Pub/Sub, Dataflow, BigQuery) <br> - Set up logging and alerts in Stackdriver <br> - CI/CD using Cloud Build for Beam jobs |

---

### 6. 🚀 Retail Analytics with FastAPI
**Stack**: `fastapi`, `mlflow`, `kubernetes`, `terraform`<br>
**Focus**: Serve machine learning insights and APIs<br>
**Goal**: Wrap analytics insights as RESTful services and deploy on cloud infra<br>

| Phase | Objective | Key Activities |
|-------|-----------|----------------|
| ML API Setup | Serve analytics insights as APIs | - Build a FastAPI service for querying KPIs or forecasts <br> - Integrate with backend ML pipeline or analytics models |
| Model Management | Deploy and version models with MLflow | - Log experiments and register models <br> - Deploy model serving endpoints via MLflow |
| Infra Deployment | Deploy scalable microservices | - Use Kubernetes (GKE or K3s) to run FastAPI + MLflow <br> - Configure auto-scaling and service discovery |
| DevOps Integration | Automate testing and delivery | - Use Terraform to provision infra <br> - GitHub Actions for CI/CD <br> - Add unit tests and contract testing for APIs |

---

## 🛠️ CI/CD & DevOps Patterns

| Stack             | CI/CD             | Infra Provisioning | Deployment       |
|------------------|-------------------|---------------------|------------------|
| DBT              | GitHub Actions    | Docker              | Local            |
| SQLMesh          | GitHub Actions    | Docker              | Local            |
| Airflow          | GitHub Actions    | Docker/K8s          | Cloud            |
| Spark            | GitHub Actions    | Terraform + EMR     | AWS              |
| Dataflow         | GitHub Actions    | Terraform + GCP     | GCP              |
| FastAPI          | GitHub Actions    | Terraform + K8s     | GKE              |

---

## 🔄 Unified Use Cases

Each stack will aim to answer the same core questions:

1. **Customer segmentation**
2. **Product recommendation opportunities**
3. **Delivery performance insights**
4. **Repeat buyer prediction**
5. **Seller performance ranking**
6. **Revenue forecasting**

---

## 🔧 Milestone Roadmap

### 🟢 High Priority (Must Have)
- [ ] Ingest and profile raw data in all stacks
- [ ] Build core dimensional models (customer, product, seller)
- [ ] Implement order revenue and review score metrics
- [ ] Set up GitHub repo with readme per stack
- [ ] Add CI/CD workflows for testing and deployment

### 🟡 Medium Priority (Should Have)
- [ ] Schedule DAGs in orchestration tools (Airflow, Beam)
- [ ] Build visualisation dashboards (Superset, Looker)
- [ ] Deploy FastAPI ML microservice
- [ ] Add ML model versioning via MLflow

### 🔵 Low Priority (Nice to Have)
- [ ] Integrate with dbt metrics layer
- [ ] Add real-time alerts (Pub/Sub or Kafka triggers)
- [ ] Compare model performance across ML stacks

---

## 📌 References

- [Kaggle Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- [DBT Documentation](https://docs.getdbt.com)
- [SQLMesh Docs](https://sqlmesh.com/)
- [Apache Airflow](https://airflow.apache.org/)
- [Apache Beam](https://beam.apache.org/)
- [Spark + EMR](https://aws.amazon.com/emr/)
- [MLflow](https://mlflow.org/)
- [Superset](https://superset.apache.org/)
