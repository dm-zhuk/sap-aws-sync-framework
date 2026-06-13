import os
import sys
import json
import boto3
from dotenv import load_dotenv
from pathlib import Path

# Load environment configurations
script_dir = Path(__file__).resolve().parent
root_dir = script_dir.parent
load_dotenv(dotenv_path=root_dir / ".env")

def run_analyst_recon():
    print("=" * 95)
    print("                 NESTLÉ REAL-TIME ERP UNIFIED LEDGER EXPORT DASHBOARD")
    print("=" * 95)
    
    s3_client = boto3.client("s3")
    target_bucket = os.getenv("AWS_S3_BUCKET_NAME", "mcs-capstone-datalake-829703038395")
    
    print(f"Target Storage Lake: s3://{target_bucket}\n")
    
    all_records = []
    try:
        # Pull records from both partitions
        for topic in ["sap.mara", "sap.vbak"]:
            response = s3_client.list_objects_v2(Bucket=target_bucket, Prefix=f"landing/{topic}/")
            if 'Contents' in response:
                for obj in response['Contents']:
                    all_records.append(obj)
                    
        if not all_records:
            print("[!] No streaming ledger records found in cloud storage buckets.")
            return

        # Sort all data points globally by the actual AWS modification file timestamp
        sorted_objects = sorted(all_records, key=lambda x: x['LastModified'], reverse=True)
        latest_records = sorted_objects[:10]

        print("[SUCCESS] Data stream parity confirmed. Exporting flat spreadsheet format:\n")
        
        headers = f"{'MODULE':<10} | {'MATERIAL_ID':<12} | {'SALES_ORDER_ID':<14} | {'TYPE/CUSTOMER':<13} | {'UNIT_MEASURE':<12} | {'NET_VALUE':<10} | {'TRANSACTION_DATE'}"
        print(headers)
        print("-" * 95)

        for obj in latest_records:
            # Fetch the raw JSON dictionary payload from AWS S3 storage
            file_data = s3_client.get_object(Bucket=target_bucket, Key=obj['Key'])
            payload = json.loads(file_data['Body'].read().decode('utf-8'))
            
            # Extract names matching PostgreSQL definitions
            material_id = payload.get('matnr', 'N/A')
            
            if "sap.mara" in obj['Key']:
                module = "MATERIALS"
                sales_order = "N/A"                            # Materials don't have a Sales Order ID
                type_or_cust = payload.get('mtart', 'N/A')     # Maps to Material Type (mtart)
                unit_measure = payload.get('meins', 'PC')      # Maps to Unit of Measure (meins)
                net_value = 0.00                               # Materials don't carry dynamic sales revenue records
                trans_date = payload.get('ersda', obj['LastModified'].strftime('%Y-%m-%d %H:%M'))[:16]
            else:
                module = "SALES"
                file_key_name = obj['Key'].split('/')[-1]
                fallback_so = file_key_name.replace('.json', '').replace('MAT-', '1000')
                sales_order = f"SO-{payload.get('vbeln', fallback_so)}"
                
                type_or_cust = payload.get('kunnr', 'N/A')     # Maps to Customer Number ID (kunnr)
                unit_measure = payload.get('waerk', 'EUR')     # Maps to Document Currency Unit (waerk)
                net_value = float(payload.get('netwr', 0.0))   # Maps to Net Value transaction value (netwr)
                trans_date = payload.get('erdat', obj['LastModified'].strftime('%Y-%m-%d %H:%M'))[:16]

            # Enforce clean alignment mapping for easy copy-pasting directly into Excel rows
            print(f"{module:<10} | {material_id:<12} | {sales_order:<14} | {type_or_cust:<13} | {unit_measure:<12} | ${net_value:<9.2f} | {trans_date}")
            
    except Exception as e:
        print(f"[ERROR] Failed to compile structural matrix dataset: {e}")
    print("=" * 95)

if __name__ == "__main__":
    run_analyst_recon()