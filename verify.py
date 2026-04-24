# verify.py
import pandas as pd
import psycopg2
from datetime import datetime

# UPDATE YOUR PASSWORD HERE
CONN_STR = 'postgresql://postgres:12345@localhost:5432/medicare_claims'
TABLE    = 'claims.part_d_prescribers'

def run_checks(conn):
    results = []

    checks = [
        # (check_name, sql_query, threshold_for_fail)
        ("null_npi",
         f"SELECT COUNT(*) FROM {TABLE} WHERE npi IS NULL",
         0),
        ("null_total_claims",
         f"SELECT COUNT(*) FROM {TABLE} WHERE total_claims IS NULL",
         100),
        ("negative_cost",
         f"SELECT COUNT(*) FROM {TABLE} WHERE total_drug_cost < 0",
         0),
        ("extreme_cost_outliers",
         f"""SELECT COUNT(*) FROM {TABLE}
             WHERE total_drug_cost > (
               SELECT AVG(total_drug_cost) + 4*STDDEV(total_drug_cost)
               FROM {TABLE}
             )""",
         500), # Threshold adjusted for 26M rows
        ("invalid_state_code",
         f"""SELECT COUNT(*) FROM {TABLE}
             WHERE LENGTH(TRIM(state)) != 2
                OR state NOT SIMILAR TO '[A-Z][A-Z]'""",
         0),
    ]

    cur = conn.cursor()
    print("Starting Automated Data Verification...")
    
    for name, sql, threshold in checks:
        cur.execute(sql)
        count = cur.fetchone()[0]
        status = 'PASS' if count <= threshold else 'FAIL'
        results.append({
            'check':     name,
            'count':     count,
            'threshold': threshold,
            'status':    status,
            'run_at':    datetime.now().isoformat()
        })
        print(f"[{status}] {name}: {count:,} records found")

    cur.close()
    return results

if __name__ == '__main__':
    conn = psycopg2.connect(CONN_STR)
    results = run_checks(conn)
    conn.close()

    df = pd.DataFrame(results)
    out = f'verification_report_{datetime.now():%Y%m%d_%H%M}.csv'
    df.to_csv(out, index=False)
    print(f'\nVerification report written to {out}')
    fail_count = (df['status'] == 'FAIL').sum()
    if fail_count:
        print(f'WARNING: {fail_count} check(s) failed. Review {out} for data issues.')