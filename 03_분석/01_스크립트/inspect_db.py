import duckdb
import sys

db_path = "/Volumes/T9/duckdb-analytics/db/apartment.duckdb"

try:
    con = duckdb.connect(db_path, read_only=True)

    ret = con.execute("SELECT 주소 FROM 공동주택_전국 LIMIT 2").fetchall()
    print("공동주택_전국 주소 샘플:", ret)

    ret = con.execute("SELECT 도로명주소 FROM 좌표 LIMIT 2").fetchall()
    print("좌표 도로명주소 샘플:", ret)

except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)
