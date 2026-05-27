# Resource-Optimized Hybrid SAP-to-AWS CDC Simulation Pipeline
## Capstone Project ID_3782250503

An asynchronous, application-level Change Data Capture (CDC) emulation framework designed to replicate core enterprise database transactions into a cloud-native AWS S3 Data Lake with sub-second latency (<150ms).

## 1. Architectural Topology & Data Flow

1. **Source Layer (PostgreSQL):** synthetic SAP transaction entries are generated and committed sequentially to relational database tables (`MARA` for Materials Master, `VBAK` for Sales Orders).
2. **Capture & Messaging Layer (Apache Kafka):** upon a verified database transaction commit, the generation engine instantly dispatches non-blocking event payloads to dedicated broker topics (`sap.mara`, `sap.vbak`).
3. **Ingestion Layer (Python Worker):** a lightweight, decoupled Kafka consumer (`streaming_job.py`) polls events continuously, formats payloads to prevent JSON serialization errors, and uploads records to AWS S3.
4. **Analytics Consumption Layer (Business Dashboard):** A modular analytics reporting script (`analyst_dashboard.py`) executes real-time extractions directly from the S3 landing buckets, preparing a flat, unified, cell-aligned data ledger optimized for Microsoft Access & Excel integration.

## 2. Repository Structure

```text
    ├── README.md                   # System operational manual
    ├── docker-compose.yaml         # Container topology (Postgres, Kafka, Zookeeper, Adminer)
    ├── .env                        # Local and AWS credential environment configurations
    ├── src
    │   ├── generator.py            # Transactional database simulator & event producer
    │   ├── kafka_streamer.py       # Core Kafka event dispatch utility thread
    │   ├── schemas.py              # DDL schema definitions and mock data factory
    │   ├── streaming_job.py        # Cloud-native ingestion worker (Kafka consumer -> AWS S3)
    │   └── analyst_dashboard.py    # Unified ledger report compiler for business analysts
    └── terraform
        └── main.tf                 # Declarative AWS S3 state configuration and import logic
```

## 3. Environment Configuration (`.env`)

For security reason, credentials are kept isolated from the source control system. To configure your local runtime:

Copy and adapt the template configuration file:
```bash
   # Database Local Target Configuration# Database Local Target Configuration
    SAP_DB_HOST=localhost
    SAP_DB_PORT=5432
    SAP_DB_NAME=sap_simulation
    SAP_DB_USER=sap_user
    SAP_DB_PASSWORD=your_secure_local_password_here

    # Kafka Message Broker Configuration
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092

    # AWS Cloud Infrastructure Configuration
    AWS_ACCESS_KEY_ID=YOUR_AWS_ACCESS_KEY_ID_HERE
    AWS_SECRET_ACCESS_KEY=YOUR_AWS_SECRET_ACCESS_KEY_HERE
    AWS_REGION=eu-central-1
    AWS_S3_BUCKET_NAME=mcs-capstone-datalake-829703038395
```

## 4. Execution Protocol

Follow these commands sequentially within your active virtual environment (venv) to run or validate the entire pipeline framework:

* **Step 1: Initialize the Infrastructure Containers**
Bring up the local database, message brokers, and resource-bounded monitoring nodes:

```Bash
docker compose up -d
```

![`docker ps`](img/docker_ps.png)

* **Step 2: Initialize or Verify the Cloud Storage Target**
Link the pre-existing AWS S3 storage lake to local state boundaries using Terraform:

```Bash
cd terraform
terraform init
terraform apply -auto-approve
cd ..
```

![`terraform apply`](img/terraform.png)

* **Step 3: Launch the Cloud Ingestion Forwarder**
Open a dedicated terminal window, navigate to the project folder, and execute the active consumer daemon script:

```Bash
source venv/bin/activate
python3 src/streaming_job.py
```

![Cloud Ingestion Forwarder](img/SAP_CDC_dual_write_framework_exec_20260526.png)

* **Step 4: Run the Transaction Generation Engine**
Open a second terminal window and execute the data simulation script (e.g., a safe 50-record validation batch):

```Bash
source venv/bin/activate
python3 src/generator.py --max-records 50
```

![Transaction Generation Engine](img/SAP_simulation_data_20260526.png)

* **Step 5: Execute the Real-Time Business Reconciliation Report**
To verify that data has safely crossed the network boundary and stands structured for business consumption, execute the analyst report compiler:

```Bash
python3 src/analyst_dashboard.py
```

![Real-Time Business Reconciliation Report](img/Business_dashboard.png)

## 5. Decommissioning & Cleanup
Storage buckets wipe out & container clusters teardown:

```Bash
# Terminate and remove local containers
docker compose down -v

# Safely destroy cloud buckets managed via Terraform
cd terraform
terraform destroy -auto-approve
```
