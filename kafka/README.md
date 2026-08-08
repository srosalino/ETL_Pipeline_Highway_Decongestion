# Highway Toll Streaming Data Pipeline

This project implements a real-time data streaming pipeline for simulated highway toll-plaza traffic.

A Python producer generates toll traffic events and publishes them to an Apache Kafka topic. A Python Kafka consumer reads the events and writes them into a MySQL database running in Docker.

## Architecture

```text
Python Toll Traffic Generator
        │
        │ publishes events
        ▼
Apache Kafka
Topic: toll
        │
        │ consumes events
        ▼
Python Streaming Consumer
        │
        │ inserts records
        ▼
MySQL
Database: tolldata
Table: livetolldata
```

Kafka runs locally on the host machine, while MySQL runs inside a Docker container.

---

## Event Schema

Each Kafka event contains:

- event timestamp
- vehicle ID
- vehicle type
- toll plaza ID

---

# Prerequisites

The following software must be installed before running the pipeline.

## 1. Git Bash

The shell scripts in this project are Bash scripts.

On Windows, run the commands below using **Git Bash**, not Command Prompt (`cmd.exe`).

Verify:

```bash
bash --version
```

---

## 2. Docker Desktop

Docker is used to run the MySQL database.

Verify that Docker Desktop is running:

```bash
docker --version
```

---

## 3. Java

Apache Kafka requires Java.

Verify:

```bash
java -version
```

If `java` is not found, install a supported JDK and ensure Java is available on the system `PATH`.

---

## 4. Python

Python is required for the Kafka producer and consumer.

Verify:

```bash
python --version
```

The project has been tested with Python 3.12.

---

# Initial Setup

The following steps are required when setting up the project for the first time.

## 1. Clone the repository

```bash
git clone <repository-url>
cd <repository-directory>
```

Run all subsequent commands from the project root.

---

## 2. Create the local environment file

Copy the provided environment template:

```bash
cp .env.example .env
```

Configure `.env` for the local environment:

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=toll

MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=tolldata
MYSQL_USER=root
MYSQL_PASSWORD=test
```

`MYSQL_HOST=localhost` is required because the Python streaming consumer runs on the Windows host and connects to the MySQL container through its published port.

Load the variables into the current Git Bash session:

```bash
source .env
```

The `run_pipeline.sh` script also loads `.env` automatically.

Do not commit `.env` if it contains passwords or other credentials.

---

## 3. Start MySQL

Create the MySQL Docker container if it does not already exist:

```bash
docker run \
    --name toll-mysql \
    -e MYSQL_ROOT_PASSWORD=test \
    -p 3306:3306 \
    -d mysql:8.4
```

Verify that the container is running:

```bash
docker ps
```

You should see a container named:

```text
toll-mysql
```

If the container already exists but is stopped, start it with:

```bash
docker start toll-mysql
```

---

## 4. Initialize the MySQL schema

The database schema can be initialized directly inside the MySQL Docker container:

```bash
docker exec \
    -e MYSQL_PWD="$MYSQL_PASSWORD" \
    -i toll-mysql \
    mysql \
    --user="$MYSQL_USER" \
    < sql/create_schema.sql
```

This creates the database and table required by the streaming consumer.

The schema only needs to be initialized when setting up a new database/container, unless the schema needs to be recreated.

---

## 5. Install Kafka

Run:

```bash
./scripts/install_kafka.sh
```

The script downloads and extracts the configured Apache Kafka distribution into:

```text
tools/
```

For example:

```text
tools/kafka_2.12-3.7.0/
```

The Kafka installation directory is generated locally and should not be committed to Git.

---

## 6. Configure Kafka storage

Kafka uses KRaft for cluster metadata and local persistent storage.

In:

```text
tools/kafka_2.12-3.7.0/config/kraft/server.properties
```

Configure `log.dirs` to point to the project's `kafka-data` directory using an absolute Windows path.

For example:

```properties
log.dirs=C:/Users/<username>/Documents/path/to/project/kafka-data
```

Do not use the default `/tmp/kraft-combined-logs` path when running Kafka through Git Bash on Windows, as Windows/Git Bash path translation can cause Kafka storage and cluster-ID problems.

Create the directory if necessary:

```bash
mkdir -p kafka-data
```

`kafka-data/` will contain Kafka's runtime state, including topic partitions, consumer offsets, and KRaft metadata. It should not be committed to Git.

---

## 7. Initialize and start Kafka

### Configure Log4j for Git Bash on Windows

When Kafka is launched from Git Bash on Windows, Kafka may incorrectly resolve the path to its `log4j.properties` configuration file.

Set the Log4j configuration explicitly:

```bash
export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:///$(pwd)/tools/kafka_2.12-3.7.0/config/log4j.properties"
```

This tells Kafka explicitly where its Log4j configuration file is located.

Run:

```bash
./scripts/initialize_kafka.sh
```

On the first execution, the script initializes the Kafka KRaft storage and creates a cluster ID.

Expected output includes:

```text
Formatting Kafka storage...
Kafka storage initialized.
Starting Kafka...
```

On subsequent executions, the existing Kafka storage should be reused rather than reformatted.

Keep this Git Bash terminal open while Kafka is running.

Kafka listens on:

```text
localhost:9092
```

---

# Running the Pipeline

Open a **second Git Bash terminal** and navigate to the project directory.

If necessary, load the environment:

```bash
source .env
```

Then run:

```bash
./scripts/run_pipeline.sh
```

The script:

1. loads the environment configuration;
2. installs/checks the Python dependencies from `requirements.txt`;
3. starts the Kafka consumer;
4. connects the consumer to MySQL;
5. starts the toll traffic generator;
6. publishes simulated traffic events to Kafka;
7. consumes those events;
8. inserts the resulting records into MySQL.

A successful run should show producer messages such as:

```text
Published car event for vehicle ... to partition 0, offset ...
```

and consumer messages such as:

```text
Inserted vehicle ... from partition 0, offset ...
```

`Published` means that the producer successfully sent an event to Kafka.

`Inserted` means that the consumer successfully read an event from Kafka and inserted it into MySQL.

---

# Stopping the Pipeline

The Kafka consumer is a long-running process and continues waiting for new Kafka messages.

Stop the pipeline with:

```text
Ctrl+C
```

Kafka can also be stopped by pressing:

```text
Ctrl+C
```

in the terminal running `initialize_kafka.sh`.

The MySQL container can be stopped separately with:

```bash
docker stop toll-mysql
```

---

# Querying MySQL

Because Git Bash uses Mintty on Windows, use `winpty` when opening an interactive MySQL session inside Docker:

```bash
winpty docker exec -it toll-mysql mysql -u root -p
```

Enter the configured MySQL password when prompted.

Then:

```sql
USE tolldata;

SHOW TABLES;

SELECT *
FROM livetolldata
LIMIT 20;
```

To count the records inserted by the Kafka consumer:

```sql
SELECT COUNT(*)
FROM livetolldata;
```

Exit MySQL with:

```sql
EXIT;
```
