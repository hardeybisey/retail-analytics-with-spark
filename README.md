## Definitions

Catalog
* Spark: how Spark resolves identifiers catalog.db.table
* Iceberg: an entrypoint to create, load, or drop tables.

Namespace
* Spark: A namespace is analogous to a database/schema. It is the grouping unit for tables within a catalog.
* Iceberg: Iceberg uses the term namespace instead of database. It’s a logical grouping of tables.

Warehouse
* Spark: spark.sql.warehouse.dir defines the root directory in a filesystem where managed table data lives.
* Iceberg: When you configure a Hadoop or file-based Iceberg catalog, the warehouse is the root directory under which namespaces and table metadata directories are stored.

# Spark Job Flow and Role Breakdown

### Driver
* The driver is the main control process for a Spark application. It:
* Translates user code (e.g., your Spark job) into a logical execution plan.
* Converts that plan into tasks.
* Manages the entire lifecycle of the Spark job.
* The driver requests resources and submits tasks to the cluster manager (e.g., the Spark master in standalone mode).
* It maintains metadata, job progress, and coordinates task scheduling.
* It does not send the entire job to the master but interacts with the cluster manager to allocate executors.

### Master (Cluster Manager)
* The master is the resource manager and scheduler.
* It allocates resources across the cluster by managing workers.
* When the driver requests executors (containers or JVM processes that run tasks), the master grants resources on workers.
* It does not distribute tasks directly but assigns executors to the driver.

### Workers
* Workers are the physical or virtual nodes that run executors.
* A worker can run multiple executors depending on the resources allocated.
* Executors execute tasks — actual units of work derived from the driver’s plan.
* Executors run in JVM processes on workers.

### Executors
* Executors receive tasks from the driver.
* They execute the tasks, perform data processing, caching, shuffle, etc.
* Each executor runs multiple tasks in threads.


Flow Summary:
Driver requests executors from Master.
Master assigns executors on available Workers.
Driver schedules tasks on those executors.
Executors run tasks and report status back to the driver.

Key Notes:
The driver manages task scheduling to executors directly; the master’s role is resource allocation.

Tasks do not pass through the master after allocation; driver ↔ executors communication is direct.

Executors are bounded by resources on workers; multiple executors can run on one worker if resources permit.


### 1. Spark treats the **driver as a “special executor”**

* In Spark, the **driver process** is the one that **coordinates the cluster**: planning the DAG, scheduling tasks, and collecting results.
* Every task in Spark runs on an executor, but the driver itself can also **run tasks locally**, especially in local mode or when small jobs are submitted from a notebook.
* To unify the handling of resources internally, Spark **assigns the driver an executor ID of `driver`**.

<!-- aws --endpoint-url http://minio:9000 s3 ls s3://spark-logs/spark-events -->
