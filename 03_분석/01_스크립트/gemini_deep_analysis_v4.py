import duckdb
import pandas as pd

con = duckdb.connect("/Volumes/T9/duckdb-analytics/db/apartment.duckdb", read_only=True)

print("--- 1. 연도별 주요 권역 평당가 (2006~2026) ---")
q1 = """
SELECT 
    CAST(계약년월 / 100 AS INTEGER) as 연도,
    CASE 
        WHEN 시도 = '서울특별시' THEN '서울'
        WHEN 시도 IN ('경기도', '인천광역시') THEN '경기/인천'
        WHEN 시도 IN ('부산광역시', '대구광역시', '광주광역시', '대전광역시', '울산광역시') THEN '지방광역시'
        ELSE '기타지방'
    END as 권역,
    MEDIAN(매매대표평당가_만원) as 평당가
FROM v_sale_monthly_yoy
WHERE 계약년월 >= 200601
GROUP BY 1, 2
ORDER BY 1, 2
"""
df1 = con.execute(q1).df().pivot(index='연도', columns='권역', values='평당가').round(0)
print(df1.to_string())

print("\n--- 2. 수도권 시군구 10급지별 연도별 평당가 (2015~2026) ---")
q2 = """
SELECT 
    CAST(a.계약년월 / 100 AS INTEGER) as 연도,
    b.급지,
    MEDIAN(a.매매대표평당가_만원) as 평당가
FROM v_sale_monthly_yoy a
JOIN read_csv_auto('02_데이터/02_참조/수도권_매매_급지표_시군구_20260311.csv') b ON a.시군구 = b.시군구 AND a.시도 IN ('서울특별시', '경기도', '인천광역시')
WHERE a.계약년월 >= 201501
GROUP BY 1, 2
"""
df2 = con.execute(q2).df().pivot(index='연도', columns='급지', values='평당가').round(0)
print(df2.to_string())

print("\n--- 3. 최근 6개월 (2025.09~2026.02) 중소형 시군구 YoY 강도 (수도권 상하위 10, 비수도권 상위 10) ---")
q3 = """
SELECT 
    CASE WHEN 시도 IN ('서울특별시', '경기도', '인천광역시') THEN '수도권' ELSE '비수도권' END as 권역,
    시도, 시군구, 
    MEDIAN(매매대표가격_YoY_pct) as YoY,
    SUM(거래건수) as 거래량
FROM v_sale_monthly_yoy
WHERE 계약년월 >= 202509 AND 계약년월 <= 202602 AND 전용면적_구분 = '중소형'
GROUP BY 1, 2, 3 HAVING SUM(거래건수) >= 50
ORDER BY 1 DESC, 4 DESC
"""
df3 = con.execute(q3).df()
sudo = df3[df3['권역'] == '수도권']
non_sudo = df3[df3['권역'] == '비수도권']
print("수도권 Top 10:\n", sudo.head(10).to_string())
print("수도권 Bottom 10:\n", sudo.tail(10).to_string())
print("비수도권 Top 10:\n", non_sudo.head(10).to_string())

print("\n--- 4. 전국 주요 권역별 준공후 미분양 흐름 (2022.12 vs 2024.12 vs 2026.01) ---")
q4 = """
SELECT 
    시점,
    CASE 
        WHEN 시도 IN ('서울', '경기', '인천') THEN '수도권'
        WHEN 시도 IN ('부산', '대구', '광주', '대전', '울산') THEN '지방광역시'
        WHEN 시도 = '전국' THEN '전국'
        ELSE '기타지방'
    END as 권역,
    SUM(CAST(준공_후_미분양수 AS INTEGER)) as 준공후미분양,
    SUM(CAST(미분양수 AS INTEGER)) as 총미분양
FROM KOSIS_미분양종합
WHERE 시점 IN ('2022.12', '2024.12', '2026.01')
GROUP BY 1, 2
ORDER BY 1, 2
"""
df4 = con.execute(q4).df()
print(df4.to_string())

