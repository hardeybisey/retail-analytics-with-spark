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
