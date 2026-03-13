import duckdb
db_path = "/Volumes/T9/duckdb-analytics/db/apartment.duckdb"
try:
    con = duckdb.connect(db_path)
    con.execute("CREATE OR REPLACE VIEW v_test_write AS SELECT 1 AS id")
    print("Write access successful")
except Exception as e:
    print(f"Failed: {e}")
