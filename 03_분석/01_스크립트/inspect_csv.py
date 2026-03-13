import duckdb
import pandas as pd


def main():
    con = duckdb.connect("/Volumes/T9/duckdb-analytics/db/apartment.duckdb")

    print("--- 교통축_인접_매핑.csv ---")
    try:
        df = con.execute(
            "SELECT * FROM read_csv_auto('02_데이터/02_참조/교통축_인접_매핑.csv') LIMIT 3"
        ).df()
        print(df)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
