import duckdb
import pandas as pd
import sys


def main():
    con = duckdb.connect(
        "/Volumes/T9/duckdb-analytics/db/apartment.duckdb", read_only=True
    )

    print("--- 수도권 최신 중소형 YoY ---")
    try:
        df_yoy = con.execute(
            """
            SELECT 시도, 시군구 as 시군구, MEDIAN(MoM) as MoM, MEDIAN(YoY) as YoY
            FROM v_sale_monthly_yoy 
            WHERE 계약년월 = 202602 AND 전용면적_구분 = '중소형' AND 시도 IN ('서울특별시', '경기도', '인천광역시')
            GROUP BY 1, 2
            ORDER BY 4 DESC
        """
        ).df()
        print("Top 5:")
        print(df_yoy.head(5))
        print("Bottom 5:")
        print(df_yoy.tail(5))
    except Exception as e:
        print(f"Error 1: {e}")

    print("\n--- 전국 미분양 흐름 ---")
    try:
        df_unsold = con.execute(
            """
            SELECT 시점, 
                   SUM(CAST(미분양수 AS INTEGER)) as 총미분양, 
                   SUM(CAST(준공_후_미분양수 AS INTEGER)) as 준공후미분양 
            FROM KOSIS_미분양종합
            WHERE CAST(시점 AS VARCHAR) LIKE '2022%' OR CAST(시점 AS VARCHAR) LIKE '2023%' OR CAST(시점 AS VARCHAR) LIKE '2024%' OR CAST(시점 AS VARCHAR) LIKE '2025%' OR CAST(시점 AS VARCHAR) LIKE '2026%'
            GROUP BY 1 ORDER BY 1 DESC LIMIT 5
        """
        ).df()
        print(df_unsold)
    except Exception as e:
        print(f"Error 2: {e}")


if __name__ == "__main__":
    main()
