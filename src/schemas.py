# ==============================================================================
# 1. DATABASE SCHEMA DEFINITIONS (DDL)
# ==============================================================================
# This tuple holds the raw SQL commands used to build the structural tables.
# Ensures the local PostgreSQL database matches core SAP structures.

DDL_COMMANDS = (
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
    """
)

# ==============================================================================
# 2. MOCK SAP DATA FACTORY
# ==============================================================================
def generate_sap_records(fake):
    """
    Generates a single, synchronized set of mock records for MARA and VBAK.
    """
    # Generate common transactional metadata values
    material_id = f"MAT-{fake.random_int(min=1000, max=9999)}"
    material_type = fake.lexify(text="????").upper()
    material_group = fake.numerify(text="###")
    customer_id = fake.numerify(text="0000######")   # SAP 10-digit standard format
    
    # Generate the price decimal for the database
    net_value_db = fake.pydecimal(left_digits=5, right_digits=2, positive=True)
    net_value_kafka = float(net_value_db)

    # Organize the database parameters into tuples for SQL insertion
    db_mara_data = (material_id, material_type, material_group, "PC")
    db_vbak_data = (customer_id, "EUR", net_value_db, material_id)

    # Build individual json-type dictionaries for kafka topics
    kafka_mara_payload = {
        "matnr": material_id,
        "mtart": material_type,
        "matkl": material_group,
        "meins": "PC"
    }
    
    kafka_vbak_payload = {
        "kunnr": customer_id,
        "waerk": "EUR",
        "netwr": net_value_kafka,
        "matnr": material_id
    }

    # Return everything back to the main engine
    return material_id, db_mara_data, db_vbak_data, kafka_mara_payload, kafka_vbak_payload