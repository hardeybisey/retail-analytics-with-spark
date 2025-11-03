* https://docs.datahub.com/docs/quickstart/
* https://docs.open-metadata.org/latest/quick-start/local-docker-deployment
* https://openlineage.io/getting-started/
* https://opentelemetry.io/docs/languages/python/
* https://grafana.com/docs/loki/latest/
* https://marquezproject.ai/


| Week  | Theme          | Key Tools                          | Deliverable                                        |
| ----- | -------------- | ---------------------------------- | -------------------------------------------------- |
| 1–3   | Data Catalogue | DataHub or OpenMetadata            | Deploy, ingest metadata, visualise lineage         |
| 4–6   | Data Lineage   | OpenLineage + Marquez              | Integrate with Airflow/dbt, generate lineage graph |
| 7–9   | Observability  | OpenTelemetry, Prometheus, Grafana | Instrument a Python service, add dashboards        |
| 10–12 | Logging        | Fluent Bit, Loki, Grafana          | Centralise logs across all containers              |

---

## **1. Data Catalogue**

**Goal:** Understand how to automatically discover, classify, and document datasets, with strong metadata management.

### Core Concepts

* **Metadata layers**: technical (schemas, lineage), operational (usage, freshness), and business (definitions, owners).
* **Data discovery and governance patterns**: active metadata vs. passive, push/pull metadata ingestion.
* **Integration with data platforms**: BigQuery, dbt, Airflow, etc.

### Hands-on Tools

| Tool                      | Focus                                | Learn by Doing                                                                        |
| ------------------------- | ------------------------------------ | ------------------------------------------------------------------------------------- |
| **DataHub** (by LinkedIn) | Active metadata, lineage, governance | Deploy via Docker Compose → Ingest metadata from dbt & Airflow → Search lineage graph |
| **OpenMetadata**          | Lineage + data quality + profiler    | Set up on Docker → Connect to BigQuery → Auto-profile and document datasets           |
| **Amundsen**              | Lightweight discovery                | Good for understanding the basic catalogue architecture                               |

### Suggested Project

> **"Metadata Portal for MSAP"**
> Deploy **DataHub** and configure ingestion from your Airflow DAGs, BigQuery datasets, and dbt models. Then:

* Add business metadata (owners, tags).
* Visualise upstream/downstream lineage from raw → curated → semantic layers.

---

## **2. Data Lineage**

**Goal:** Trace how data flows and transforms across pipelines, to support debugging, compliance, and governance.

### Core Concepts

* **Types**: Technical lineage (table → column), logical lineage (models → reports), operational lineage (jobs → runs).
* **Lineage extraction patterns**: parsing SQL, compiler hooks (e.g. dbt artifacts), orchestration hooks (Airflow operators).
* **Data contracts & schema versioning** as lineage stabilisers.

### Hands-on Tools

* **dbt + DataHub lineage** → get table/column-level lineage automatically.
* **OpenLineage + Marquez** → learn standardised lineage capture from jobs across Spark, Airflow, and dbt.
* **Great Expectations or Soda** → enrich lineage with quality events.

### Suggested Exercise

Integrate **OpenLineage** with **Airflow** and **dbt** in a local setup.
Each DAG run should produce lineage events → stored in **Marquez UI** → view dependencies between raw, staging, and mart tables.

---

## **3. Application Observability**

**Goal:** Gain visibility into the performance and health of your pipelines and services.

### Core Concepts

* **Three pillars:** Metrics, Traces, Logs.
* **Distributed tracing** for data pipelines (understanding latency and bottlenecks).
* **Instrumentation standards** like **OpenTelemetry**.
* **Metrics aggregation** (Prometheus) and visualisation (Grafana).

### Hands-on Tools

| Tool                     | Role                                | Learn by Doing                                                |
| ------------------------ | ----------------------------------- | ------------------------------------------------------------- |
| **Prometheus + Grafana** | Metrics collection & dashboarding   | Monitor Airflow DAGs, Spark jobs, or backend API performance  |
| **OpenTelemetry**        | Tracing and metrics instrumentation | Add tracing spans in your Python ETL or Flask/FastAPI backend |
| **Jaeger**               | Distributed trace visualisation     | Visualise end-to-end flow across services                     |

### Suggested Exercise

Add **OpenTelemetry instrumentation** to your Python API (e.g. MSAP backend).
Export traces to **Jaeger** and metrics to **Prometheus**. Create a **Grafana dashboard** for pipeline latency and success rates.

---

## **4. Logging and Monitoring**

**Goal:** Implement structured, centralised, and queryable logging across all services.

### Core Concepts

* **Structured logging** (JSON logs with context keys).
* **Log aggregation and parsing** (ELK/EFK stacks).
* **Correlation IDs** for tracing data across systems.

### Hands-on Tools

| Tool                                                        | Use Case                                     |
| ----------------------------------------------------------- | -------------------------------------------- |
| **Fluent Bit or Filebeat** → **Elasticsearch** → **Kibana** | Centralised log aggregation and search       |
| **Loki + Grafana**                                          | Lightweight alternative for logs with labels |
| **Google Cloud Logging** (if using GCP)                     | Seamless integration with GKE and Dataflow   |

### Suggested Exercise

Containerise your services → Add **Fluent Bit sidecar** for log forwarding → Index logs in **Loki** → Query via Grafana.

---

## **5. Suggested Learning Sequence (12 Weeks)**

| Week  | Theme          | Key Tools                          | Deliverable                                        |
| ----- | -------------- | ---------------------------------- | -------------------------------------------------- |
| 1–3   | Data Catalogue | DataHub or OpenMetadata            | Deploy, ingest metadata, visualise lineage         |
| 4–6   | Data Lineage   | OpenLineage + Marquez              | Integrate with Airflow/dbt, generate lineage graph |
| 7–9   | Observability  | OpenTelemetry, Prometheus, Grafana | Instrument a Python service, add dashboards        |
| 10–12 | Logging        | Fluent Bit, Loki, Grafana          | Centralise logs across all containers              |

By week 12, you should have a **fully observable data ecosystem**:

* Metadata portal (DataHub)
* Lineage explorer (Marquez)
* Metrics dashboards (Grafana)
* Logs & traces (Loki + Jaeger)

---

## **Recommended References**

* [DataHub Quickstart](https://datahubproject.io/docs/quickstart/)
* [OpenMetadata Deployment](https://docs.open-metadata.org/)
* [OpenLineage + Airflow Integration](https://openlineage.io/integration/airflow/)
* [OpenTelemetry for Python](https://opentelemetry.io/docs/instrumentation/python/)
* [Grafana Loki Stack](https://grafana.com/docs/loki/latest/)
* [Marquez Lineage Project](https://marquezproject.github.io/marquez/)

---

**Follow Up:**
Would you like me to design a 12-week *hands-on roadmap* with weekly objectives, architecture diagrams, and Docker Compose setups to deploy each tool locally and integrate them into one cohesive stack?


docker network create minikube --subnet=172.42.0.0/16 --gateway=172.42.0.1

minikube start \
  --cpus='6' \
  --nodes='1' \
  --memory='no-limit' \
  --disk-size='50gb' \
  --driver='docker' \
  --network='minikube' \
  --addons=ingress,metrics-server \
  --mount-string ${HOME}/Desktop/workspace/projects/retail-analytics-with-spark/airflow:/home/airflow --mount

minikube mount ${HOME}/Desktop/workspace/projects/retail-analytics-with-spark/airflow:/home/airflow

minikube ip
172.42.0.2

mk addons enable ingress

kubectl label nodes minikube node.kubernetes.io/exclude-from-external-load-balancers-

mkctl apply -Rf /Users/hardey/Desktop/workspace/projects/retail-analytics-with-spark/deployments/kubernetes/airflow


spark-submit --master yarn --conf spark.driver.memory=512m --name run_migrations_v1 /opt/spark/spark_jobs/migrations_v1.py


| Scenario                               | Behaviour                                 | Outcome                                    |
| -------------------------------------- | ----------------------------------------- | ------------------------------------------ |
| `storageClassName` omitted             | PVC triggers dynamic provisioning         | PV is auto-created (Minikube default path) |
| `storageClassName` set to `"standard"` | PVC requests the “standard” dynamic class | Same as above, provisioned dynamically     |
| `storageClassName: ""`                 | PVC disables dynamic provisioning         | PVC binds only to pre-defined PVs          |


🧠 Key Insight

PVC-first works only when a dynamic provisioner is available through a StorageClass.
Otherwise, the PVC stays in Pending state indefinitely, waiting for a matching PV.

| Use Case                                   | Recommended Approach                            |
| ------------------------------------------ | ----------------------------------------------- |
| Local development (Minikube, hostPath)     | Static PV + PVC (`storageClassName: ""`)        |
| Cloud clusters (GKE, EKS, AKS)             | PVC only (dynamic provisioning via CSI drivers) |
| Shared NFS volumes or network file systems | Static PV + PVC (you control NFS endpoint)      |



setup postgres
setup redis
setup migrate database job
setup create user job

kubectl get secret app-secrets -o yaml


kubectl create secret generic app-secrets --from-env-file=/Users/hardey/Desktop/workspace/projects/retail-analytics-with-spark/deployments/docker/.env --dry-run=client -o yaml > secret.yaml


airflow connections add spark_conn --conn-type "spark" --conn-host spark://spark-svc --conn-port 7077


minikube start \
  --cpus='4' \
  --memory='no-limit' \
  --disk-size='50gb' \
  --driver='docker' \
  --mount-string ${HOME}/Desktop/workspace/projects/retail-analytics-with-spark/airflow:/home/airflow --mount

minikube mount ${HOME}/Desktop/workspace/projects/retail-analytics-with-spark/airflow:/home/airflow

mk service api-server-svc -n airflow

mkctl cluster-info

Kubernetes control plane is running at https://127.0.0.1:54990
CoreDNS is running at https://127.0.0.1:54990/api/v1/namespaces/kube-system/services/kube-dns:dns/proxy

To further debug and diagnose cluster problems, use 'kubectl cluster-info dump'.


---
<!--
## **5. Notes**

* Both Airflow and Spark services rely on shared environment variables defined in `.env`.
* The first startup may take longer as Docker pulls required images.
* You can monitor logs with:

  ```bash
  docker compose -f deployments/docker/docker-compose.airflow.yml logs -f
  ```
* If you modify the `.env` file, restart the containers for changes to take effect.


--master $SPARK_MASTER_URL

spark-submit --executor-memory 1G \
     --executor-cores 1 \
     --total-executor-cores 1  \
     /opt/airflow/scripts/test.py

spark-submit --executor-memory 1G \
     --executor-cores 1 \
     --total-executor-cores 1  \
     /opt/airflow/scripts/migrations_v1.py


docker buildx build --platform linux/amd64,linux/arm64  -t iadebisi/airflow-spark --target airflow --push .
docker buildx build --platform linux/amd64,linux/arm64  -t iadebisi/iceberg-spark --target spark --push .

--- -->
