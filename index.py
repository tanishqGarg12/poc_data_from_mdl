
print("Script started")
import trino
from trino.auth import BasicAuthentication
import pandas as pd

conn = trino.dbapi.connect(
    host='',
    port=,
    user='',
    catalog='',
    schema='',
    auth=BasicAuthentication("", "")
)
print(conn)

try:
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    cursor.fetchall()  # Fetch to ensure the connection is valid
    print("Authentication and connection successful.")
    cursor.execute("SELECT * FROM ref_campaign_prepared LIMIT 10")
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    print("First 10 rows from ref_campaign_prepared:")
    for row in rows:
        print(row)

    # Export to Parquet
    df = pd.DataFrame(rows, columns=columns)
    df.to_parquet("output.parquet", index=False)
    print("Data exported to output.parquet")

except Exception as e:
    print("Authentication failed:", e)
    exit(1)
