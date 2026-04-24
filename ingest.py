# ingest.py
import pandas as pd
import psycopg2
import logging
from tqdm import tqdm
import io

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

COLUMN_MAP = {
    'Prscrbr_NPI':           'npi',
    'Prscrbr_Last_Org_Name': 'provider_name',
    'Prscrbr_City':          'city',
    'Prscrbr_State_Abrvtn':  'state',
    'Prscrbr_Type':          'specialty',
    'Brnd_Name':             'brand_name',
    'Gnrc_Name':             'generic_name',
    'Tot_Clms':              'total_claims',
    'Tot_Drug_Cst':          'total_drug_cost',
    'Tot_Benes':             'total_benes',
}

def load(csv_path: str, conn_str: str, chunk_size: int = 100_000):
    logging.info("Connecting to database...")
    conn = psycopg2.connect(conn_str)
    cur  = conn.cursor()
    total = 0

    logging.info("Starting High-Speed Ingestion using COPY...")
    
    # We can use MUCH bigger chunks now: 100,000 rows at a time
    chunk_iterator = pd.read_csv(csv_path, chunksize=chunk_size, dtype=str)
    
    for chunk in tqdm(chunk_iterator, desc="Processing Chunks (100k rows)", unit="chunk"):
        # Select and rename columns
        chunk = chunk[list(COLUMN_MAP.keys())].rename(columns=COLUMN_MAP)

        # Clean numeric columns
        for col in ('total_claims', 'total_drug_cost', 'total_benes'):
            chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
        
        # Create an in-memory CSV buffer
        buffer = io.StringIO()
        # Write the chunk to the buffer without headers or indexes
        chunk.to_csv(buffer, index=False, header=False, na_rep='')
        buffer.seek(0)
        
        # Use Postgres COPY command from the memory buffer
        try:
            cur.copy_expert("""
                COPY claims.part_d_prescribers 
                (npi, provider_name, city, state, specialty, brand_name, generic_name, total_claims, total_drug_cost, total_benes) 
                FROM STDIN WITH CSV
            """, buffer)
            conn.commit()
            total += len(chunk)
        except Exception as e:
            logging.error(f"Error during COPY: {e}")
            conn.rollback()
            break

    cur.close()
    conn.close()
    logging.info(f"Ingestion complete! Successfully loaded {total:,} rows.")

if __name__ == '__main__':
    csv_path = r"C:\Users\parga\Downloads\Medicare Part D Prescribers - by Provider and Drug\Medicare Part D Prescribers - by Provider and Drug\2023\MUP_DPR_RY25_P04_V10_DY23_NPIBN.csv"
    
    # PUT YOUR POSTGRES PASSWORD HERE
    conn_str = 'postgresql://postgres:12345@localhost:5432/medicare_claims'
    
    load(csv_path, conn_str)