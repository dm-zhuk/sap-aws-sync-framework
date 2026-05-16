import os
import sys
import time
import signal
import argparse
import logging
import psycopg2
from contextlib import contextmanager
from pathlib import Path
from faker import Faker
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

script_dir = Path(__file__).resolve().parent
root_dir = script_dir.parent
dotenv_path = root_dir / ".env"
load_dotenv(dotenv_path=dotenv_path)

fake = Faker()

running = True


def handle_signal(signum, frame):
    global running
    logging.info(f"Signal {signum} received. Safe shutdown initiated...")
    running = False


# Register system signal listeners
signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = psycopg2.connect(
            host=os.getenv("SAP_DB_HOST", "localhost"),
            port=os.getenv("SAP_DB_PORT", "5432"),
            database=os.getenv("SAP_DB_NAME"),
            user=os.getenv("SAP_DB_USER"),
            password=os.getenv("SAP_DB_PASSWORD"),
            connect_timeout=5,
        )
        yield conn
    except psycopg2.Error as e:
        logging.error(f"Database connection failure: {e}")
        raise
    finally:
        if conn:
            conn.close()


def init_sap_tables():
    commands = (
        """
        CREATE TABLE IF NOT EXISTS mara (
            matnr VARCHAR(18) PRIMARY KEY,
            ersda TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            mtart VARCHAR(4),
            matkl VARCHAR(9),
            meins VARCHAR(3)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS vbak (
            vbeln SERIAL PRIMARY KEY,
            erdat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            kunnr VARCHAR(10),
            waerk VARCHAR(5),
            netwr DECIMAL(15,2),
            matnr VARCHAR(18) REFERENCES mara(matnr)
        )
        """,
    )
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for command in commands:
                    cur.execute(command)
                conn.commit()
        logging.info("[GHOST SAP] Tables MARA and VBAK verified successfully.")
    except Exception as e:
        logging.critical(f"Failed to initialize database schema: {e}")
        sys.exit(1)


def generate_job(max_records: int, duration: int, delay: float):
    """Simulates real-time transactions with strict safety boundaries."""
    global running
    records_count = 0
    start_time = time.time()

    logging.info(
        f"Starting simulation. Limits -> Max Records: {max_records if max_records > 0 else 'Uncapped'}, "
        f"Duration: {duration if duration > 0 else 'Uncapped'}s"
    )

    # Keep connection out of the loop
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                while running:
                    # Condition 1: Check record limit boundary
                    if max_records > 0 and records_count >= max_records:
                        logging.info(
                            f"Reached specified target threshold of {max_records} records."
                        )
                        break

                    # Condition 2: Check execution time boundary
                    if duration > 0 and (time.time() - start_time) >= duration:
                        logging.info(
                            f"Execution timeframe of {duration} seconds reached."
                        )
                        break

                    try:
                        mat_id = f"MAT-{fake.random_int(min=1000, max=9999)}"

                        # 1. Create a new material (MARA)
                        cur.execute(
                            "INSERT INTO mara (matnr, mtart, matkl, meins) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                            (
                                mat_id,
                                fake.lexify(text="????").upper(),
                                fake.numerify(text="###"),
                                "PC",
                            ),
                        )

                        # 2. Create a linked Sales Order (VBAK)
                        cur.execute(
                            "INSERT INTO vbak (kunnr, waerk, netwr, matnr) VALUES (%s, %s, %s, %s)",
                            (
                                fake.numerify(text="0000######"),
                                "EUR",
                                fake.pydecimal(
                                    left_digits=5, right_digits=2, positive=True
                                ),
                                mat_id,
                            ),
                        )

                        conn.commit()
                        records_count += 1
                        logging.info(
                            f"[TRANSACTION #{records_count}] Sales Order processed for {mat_id}"
                        )

                    except psycopg2.DatabaseError as db_err:
                        logging.error(
                            f"Transaction failed, rolling back step. Error: {db_err}"
                        )
                        conn.rollback()

                    # Safe interval sleep, manageable
                    time.sleep(delay)

    except Exception as e:
        logging.error(f"Execution engine encountered an unexpected error: {e}")
    finally:
        logging.info(
            f"Engine stopped. Total records ingested during session: {records_count}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Safe SAP-AWS Data Sync Framework Generation Tool"
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=100,
        help="Max transaction count to run before exit (0 for infinite)",
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

    init_sap_tables()
    generate_job(max_records=args.max_records, duration=args.duration, delay=args.delay)


# Run a quick batch of 50 records only and stop automatically
# python3 generator.py --max-records 50

# Run for exactly 2 min and then clean shutdown
# python3 generator.py --max-records 0 --duration 120
