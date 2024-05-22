# MicroStrategy with Databricks

Databricks is a unified, open analytics platform for building, deploying, sharing, and maintaining enterprise-grade data, analytics, and AI solutions at scale. The Databricks Data Intelligence Platform integrates with cloud storage and security in your cloud account, and manages and deploys cloud infrastructure on your behalf.

Databricks SQL is the collection of services that bring data warehousing capabilities and performance to your existing data lakes. Databricks SQL supports open formats and standard ANSI SQL. Databricks SQL provides general compute resources that are executed against the tables in the lakehouse. Databricks SQL is powered by SQL warehouses, offering scalable SQL compute resources decoupled from storage (a SQL warehouse is a compute resource that lets you query and explore data on Databricks). 

Databricks SQL supports many third party BI and visualization tools that can connect to SQL warehouses, including MicroStrategy.

To setup your Databricks environment, mount AWS S3 bucket with Parquet source files to the Databricks File System (DBFS) first. Databricks mounts create a link between a workspace and cloud object storage, which enables you to interact with cloud object storage using familiar file paths relative to the Databricks file system. Mounts work by creating a local alias under the /mnt directory (Databricks SQL scripts assume _/mnt/mount_s3_ location). After mounting the object storage, run SQL scripts to create aviation database schemas, external tables and views.
