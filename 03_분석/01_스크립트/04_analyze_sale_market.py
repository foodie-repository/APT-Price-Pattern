import duckdb
import pandas as pd

def main():
    con = duckdb.connect("02_데이터/03_가공/analysis.duckdb", read_only=True)
    
    # Check if necessary tables are attached
    try:
        source_db = "/Volumes/T9/duckdb-analytics/db/apartment.duckdb"
        con.execute(f"ATTACH IF NOT EXISTS '{source_db}' AS src (READ_ONLY)")
    except Exception as e:
        print(f"Attach error: {e}")
        
    print("=== 1. 수도권 급지별 최근 가격 추이 ===")
    try:
        # Load 수도권 매매 급지표
        con.execute("CREATE TEMP TABLE tmp_metro_class AS SELECT * FROM read_csv_auto('02_데이터/02_참조/수도권_매매_급지표_시군구_20260311.csv')")
        
        # Calculate monthly average price by class (excluding some extremes)
        q = """
            SELECT 
                b.급지,
                a.계약년월,
                COUNT(*) as 거래량,
                MEDIAN(a.평당매매가격) as 평당매매가격_중앙값
            FROM t_sale_monthly_px a
            JOIN v_sale_clean v ON a.지번주소 = v.지번주소 AND a.계약년월 = v.계약년월 AND a.단지명 = v.단지명_공백제거 AND a.전용면적_구분 = v.전용면적_구분
            JOIN tmp_metro_class b ON v.파싱_시군구 = b.시군구
            WHERE a.계약년월 >= 202301 AND a.계약년월 <= 202603
            GROUP BY 1, 2
            ORDER BY 1, 2
        """
        df_metro_class = con.execute(q).df()
        
        # Calculate YoYs and MoMs for recent months
        q2 = """
            WITH class_monthly AS (
                SELECT 
                    b.급지,
                    a.계약년월,
                    MEDIAN(a.평당매매가격) as px
                FROM t_sale_monthly_px a
                JOIN v_sale_clean v ON a.지번주소 = v.지번주소 AND a.계약년월 = v.계약년월 AND a.단지명 = v.단지명_공백제거 AND a.전용면적_구분 = v.전용면적_구분
                JOIN tmp_metro_class b ON v.파싱_시군구 = b.시군구
                WHERE a.계약년월 >= 202301
                GROUP BY 1, 2
            )
            SELECT 
                급지,
                계약년월,
                px,
                LAG(px) OVER (PARTITION BY 급지 ORDER BY 계약년월) as prev_px,
                px / NULLIF(LAG(px) OVER (PARTITION BY 급지 ORDER BY 계약년월), 0) - 1 as MoM
            FROM class_monthly
            ORDER BY 1, 2
        """
        df_metro_trend = con.execute(q2).df()
        
        df_recent = df_metro_trend[df_metro_trend['계약년월'] >= 202510].copy()
        print("\n수도권 급지별 최근 가격 추이 (2025.10 ~ 2026.03)")
        print(df_recent.groupby('급지').tail(1)) # Show latest 

    except Exception as e:
        print("Error in metro class analysis:", e)
        
    print("\n=== 2. 지방 광역시 / 주요중소도시 가격 추이 ===")
    try:
        # 광역시
        con.execute("CREATE TEMP TABLE tmp_metro_city AS SELECT * FROM read_csv_auto('02_데이터/02_참조/지방광역시_매매_상대서열표_시군구_20260311.csv')")
        q3 = """
            SELECT 
                b.시도, b.상대서열,
                a.계약년월,
                MEDIAN(a.평당매매가격) as px
            FROM t_sale_monthly_px a
            JOIN v_sale_clean v ON a.지번주소 = v.지번주소 AND a.계약년월 = v.계약년월 AND a.단지명 = v.단지명_공백제거 AND a.전용면적_구분 = v.전용면적_구분
            JOIN tmp_metro_city b ON v.파싱_시군구 = b.시군구
            WHERE a.계약년월 >= 202510
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
        """
        df_regional = con.execute(q3).df()
        print("\n지방광역시 상대서열별 가격 (최근)")
        print(df_regional.groupby(['시도', '상대서열']).tail(1))
    except Exception as e:
        print("Error in regional analysis:", e)
        
    print("\n=== 3. 현재 호가와 실거래 괴리 확인 (수도권 매물) ===")
    try:
        q4 = """
            SELECT 
                실질급매_판정,
                COUNT(*) as 매물수,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as 비중_퍼센트
            FROM v_naver_hoga_vs_actual
            GROUP BY 1
            ORDER BY 2 DESC
        """
        df_hoga = con.execute(q4).df()
        print("\n수도권 매물 대비 실질급매 비중")
        print(df_hoga)
    except Exception as e:
        print("Error in hoga analysis:", e)
        
    print("\n=== 4. 상품성 (연식) 에 따른 차이 ===")
    try:
        q5 = """
            SELECT 
                v.연식_구분,
                COUNT(*) as 최근6개월_거래량,
                MEDIAN(a.평당매매가격) as 최근6개월_평당평균가
            FROM t_sale_monthly_px a
            JOIN v_sale_clean v ON a.지번주소 = v.지번주소 AND a.계약년월 = v.계약년월 AND a.단지명 = v.단지명_공백제거 AND a.전용면적_구분 = v.전용면적_구분
            WHERE a.계약년월 >= 202510
              AND v.시도 IN ('서울특별시', '경기도', '인천광역시')
            GROUP BY 1
            ORDER BY 
                CASE 
                    WHEN v.연식_구분 = '5년미만' THEN 1
                    WHEN v.연식_구분 = '5~10년미만' THEN 2
                    WHEN v.연식_구분 = '10~20년미만' THEN 3
                    WHEN v.연식_구분 = '20~30년미만' THEN 4
                    WHEN v.연식_구분 = '30년이상' THEN 5
                    ELSE 6
                END
        """
        df_age = con.execute(q5).df()
        print("\n수도권 연식별 거래 및 평당가")
        print(df_age)
    except Exception as e:
        print("Error in age analysis:", e)

if __name__ == '__main__':
    main()
