import duckdb
import pandas as pd

con = duckdb.connect("/Volumes/T9/duckdb-analytics/db/apartment.duckdb", read_only=True)

# 1. 2006~2026 수도권 vs 비수도권 장기 추세 (연도별 매매대표가격_만원)
q1 = """
SELECT 
    CAST(계약년월 / 100 AS INTEGER) as 연도,
    CASE WHEN 시도 IN ('서울특별시', '경기도', '인천광역시') THEN '수도권' ELSE '비수도권' END as 권역,
    MEDIAN(매매대표평당가_만원) as 평당가_중앙값,
    SUM(거래건수) as 연간_거래량
FROM v_sale_monthly_yoy
WHERE 계약년월 >= 200601
GROUP BY 1, 2
ORDER BY 1, 2
"""
df_long_term = con.execute(q1).df()
print("--- 1. 장기 추세 ---")
print(df_long_term.head(10))

# 2. 최근 1년 (2025.03~2026.02) 시도별 평균 YoY 
q2 = """
SELECT 
    시도,
    MEDIAN(매매대표가격_YoY_pct) as 중위_YoY_pct,
    SUM(거래건수) as 거래량
FROM v_sale_monthly_yoy
WHERE 계약년월 IN (202512, 202601, 202602)
  AND 전용면적_구분 = '중소형'
GROUP BY 1
ORDER BY 2 DESC
"""
df_recent_sido = con.execute(q2).df()
print("\n--- 2. 최근 3개월 시도별 요약 ---")
print(df_recent_sido)

# 3. 10급지별 최근 5년 흐름
q3 = """
SELECT 
    b.급지,
    CAST(a.계약년월 / 100 AS INTEGER) as 연도,
    MEDIAN(a.매매대표평당가_만원) as 평당가
FROM v_sale_monthly_yoy a
JOIN read_csv_auto('02_데이터/02_참조/수도권_매매_급지표_시군구_20260311.csv') b ON a.시군구 = b.시군구 AND a.시도 IN ('서울특별시', '경기도', '인천광역시')
WHERE a.계약년월 >= 201801
GROUP BY 1, 2
ORDER BY 2, 1
"""
df_class = con.execute(q3).df()
print("\n--- 3. 수도권 급지별 흐름 ---")
print(df_class.head(20))

