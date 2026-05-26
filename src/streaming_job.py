import os
import sys
import json
import time
import signal
import logging
import boto3

from dotenv import load_dotenv
from pathlib import Path
from confluent_kafka import Consumer, KafkaError

#  Runtime logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# Resolve execution paths and environmental variables
script_dir = Path(__file__).resolve().parent
root_dir = script_dir.parent
load_dotenv(dotenv_path=root_dir / ".env")

# Global lifecycle flag for terminal exit
job_running = True

def handle_shutdown(signum, frame):
    global job_running
    logging.info("Shutdown signal caught. Safely stopping the cloud forwarder...")
    job_running = False

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

def run_cloud_forwarder():
    global job_running
    
    # 1. Initialize AWS client session
    s3_client = boto3.client("s3")
    target_bucket = os.getenv("AWS_S3_BUCKET_NAME", "mcs-capstone-datalake-829703038395")
    
    # 2. Configure Kafka Consumer
    consumer_config = {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        'group.id': 'sap-cloud-ingestion-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    
    consumer = Consumer(consumer_config)
    
    # Subscribe to target SAP modules topics
    target_topics = ['sap.mara', 'sap.vbak']
    consumer.subscribe(target_topics)
    logging.info(f"[CLOUD INGESTION] Consumer active. Listening on topics: {target_topics}")

    try:
        while job_running:
            # Poll for new messages with 1-sec timeout window
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(f"Kafka consumer engine error: {msg.error()}")
                    break

            # Process new transaction event
            topic_name = msg.topic()
            message_key = msg.key().decode('utf-8') if msg.key() else f"raw-{int(time.time())}"
            payload_data = json.loads(msg.value().decode('utf-8'))
            
            logging.info(f"[EVENT DETECTED] Processing record {message_key} from topic {topic_name}")

            # Enterprise storage partition layout: topic_name/year-month-day/key.json
            current_date = time.strftime("%Y-%m-%d")
            s3_storage_path = f"landing/{topic_name}/date={current_date}/{message_key}.json"

            try:
                # Direct upload to AWS S3 storage lake
                s3_client.put_object(
                    Bucket=target_bucket,
                    Key=s3_storage_path,
                    Body=json.dumps(payload_data, indent=2),
                    ContentType="application/json"
                )
                logging.info(f" -> [CLOUD SUCCESS] Safely uploaded to S3: s3://{target_bucket}/{s3_storage_path}")
                
            except Exception as s3_err:
                logging.error(f" -> [CLOUD FAILURE] Failed to write event to AWS S3: {s3_err}")

    except Exception as ex:
        logging.critical(f"Critical breakdown in streaming job pipeline: {ex}")
    finally:
        # Socket resource cleanup
        consumer.close()
        logging.info("[CLOUD INGESTION] Cloud forwarder shut down cleanly.")

if __name__ == "__main__":
    run_cloud_forwarder()