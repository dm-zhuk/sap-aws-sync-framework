import os
import json
from confluent_kafka import Producer

# Kafka streaming initialize
_kafka_config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'client.id': 'sap-ghost-streamer',
    'acks': '1' # Ensures broker acknowledgement without network lags
}

# Internal producer instance: for reusage of TCP connections
_producer = Producer(_kafka_config)

def _delivery_report(err, msg):
    """Internal callback to log streaming success or track failures."""
    if err is not None:
        print(f" [KAFKA ERROR] Delivery failed: {err}")
    else:
        print(f" [KAFKA PRODUCER] Event delivered to -> {msg.topic()} [Partition: {msg.partition()}]")

def send_sap_event(topic: str, key: str, payload: dict):
    """
    Dispatches transactional JSON payloads to specified Kafka topics.
    """
    try:
        _producer.produce(
            topic=topic,
            key=key,
            value=json.dumps(payload),
            callback=_delivery_report
        )
        _producer.flush()
    except Exception as e:
        print(f" [KAFKA CRITICAL] Pipeline disruption: {e}")