import os
import sys
import time
import signal
import argparse
import logging
import psycopg2

from dotenv import load_dotenv
from faker import Faker
from pathlib import Path

from schemas import DDL_COMMANDS, generate_sap_records
from kafka_streamer import send_sap_event

# Configure console logging outputs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# Path resolution to catch up .env credentials
script_dir = Path(__file__).resolve().parent
root_dir = script_dir.parent
dotenv_path = root_dir / ".env"
load_dotenv(dotenv_path=dotenv_path)

fake = Faker()

running = True


def handle_signal(signum, frame):
    """To catch terminal shutdown signals (like Ctrl/C)."""
    global running
    logging.info(f"Signal {signum} received. Safely spinning down the engine..")
    running = False


# Register signal handlers to prevent terminal crashes
signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


def get_raw_db_connection():
    """Establishes connection to PostgreSQL sap-db."""
    return psycopg2.connect(
        host=os.getenv("SAP_DB_HOST", "localhost"),
        port=os.getenv("SAP_DB_PORT", "5432"),
        database=os.getenv("SAP_DB_NAME"),
        user=os.getenv("SAP_DB_USER"),
        password=os.getenv("SAP_DB_PASSWORD"),
        connect_timeout=5,
    )


def init_sap_tables():
    """Runs once at script startup to build DDL table structures."""
    conn = None
    try:
        conn = get_raw_db_connection()
        cur = conn.cursor()
        
        # Loop through and execute the decoupled DDL commands
        for command in DDL_COMMANDS:
            cur.execute(command)
            
        conn.commit()
        cur.close()
        logging.info("[GHOST SAP] Tables MARA and VBAK verified successfully inside Postgres.")
        
    except Exception as e:
        logging.critical(f"Failed to initialize database schema: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()


def generate_job(max_records: int, duration: int, delay: float):
    """
    Execution loop - injects relational mock records into 
    Postgres and immediately streams copies into Kafka topics.
    """
    global running
    records_count = 0
    start_time = time.time()

    logging.info(
        f"Starting simulation. Constraints -> Max Records: {max_records if max_records > 0 else 'Uncapped'}, "
        f"Duration: {duration if duration > 0 else 'Uncapped'}s, Pacing: {delay}s"
    )

    conn = None
    try:
        # Establish a single, robust database session connection for the loop
        conn = get_raw_db_connection()
        cur = conn.cursor()

        while running:
            # CHECKUP 1: stops if target count reached
            if max_records > 0 and records_count >= max_records:
                logging.info(f"Target threshold of {max_records} records successfully processed.")
                break

            # CHECKUP 2: stops if the allowed running time expires (in seconds)
            if duration > 0 and (time.time() - start_time) >= duration:
                logging.info(f"Execution timeframe restriction of {duration} seconds reached.")
                break

            try:
                # Use the schemas factory to build a synced set of data elements
                mat_id, db_mara, db_vbak, kafka_mara, kafka_vbak = generate_sap_records(fake)

                # PHASE A: WRITE TO DB CORE
                cur.execute(
                    "INSERT INTO mara (matnr, mtart, matkl, meins) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                    db_mara
                )
                cur.execute(
                    "INSERT INTO vbak (kunnr, waerk, netwr, matnr) VALUES (%s, %s, %s, %s)",
                    db_vbak
                )
                
                # Commit to ensure persistence is complete
                conn.commit()
                records_count += 1
                logging.info(f"[DB COMMIT #{records_count}] Saved data state for {mat_id}")

                # PHASE B: EVENT INGESTION LAYER (Apache Kafka)
                # Runs only if Phase A succeeded.
                send_sap_event(topic="sap.mara", key=mat_id, payload=kafka_mara)
                send_sap_event(topic="sap.vbak", key=mat_id, payload=kafka_vbak)

            except psycopg2.DatabaseError as db_err:
                logging.error(f"Database write failed. Rolling back transaction step. Error: {db_err}")
                conn.rollback()

            time.sleep(delay)

        # Close active cursors when exiting the loop
        cur.close()

    except Exception as e:
        logging.error(f"Execution engine encountered an unexpected runtime error: {e}")
    finally:
        if conn:
            conn.close()
        logging.info(f"Engine shut down OK. Total records ingested during session: {records_count}")


if __name__ == "__main__":
    # Manageable runtime argument controls
    parser = argparse.ArgumentParser(
        description="Safe SAP-AWS Data Sync Framework Generation Tool"
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=100,
        help="Max transaction count to run before exit",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=0,
        help="Total seconds to let script run before exit (0 for infinite)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=2.0,
        help="Delay interval between sync transactions",
    )
    args = parser.parse_args()

    # Launch initialization and run the pipeline job
    init_sap_tables()
    generate_job(max_records=args.max_records, duration=args.duration, delay=args.delay)


# runs a batch of 50 records only and stops:
# python3 ingestion-pipeline/generator.py --max-records 50

# runs for 2 min and then clean shutdown:
# python3 ingestion-pipeline/generator.py --max-records 0 --duration 120
