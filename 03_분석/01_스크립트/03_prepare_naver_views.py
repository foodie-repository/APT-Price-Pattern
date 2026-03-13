import duckdb
import sys


def main():
    db_path = "02_데이터/03_가공/analysis.duckdb"
    con = duckdb.connect(db_path)

    source_db = "/Volumes/T9/duckdb-analytics/db/apartment.duckdb"
    try:
        con.execute(f"ATTACH IF NOT EXISTS '{source_db}' AS src (READ_ONLY)")
    except Exception as e:
        pass

    try:
        # 1. 네이버부동산 매물 스냅샷 (최신 수집일자 기준)
        print("Creating v_naver_maemul_snapshot...")
        con.execute(
            """
            CREATE OR REPLACE VIEW v_naver_maemul_snapshot AS
            SELECT *
            FROM src.네이버부동산_매물
            WHERE 수집일자 = (SELECT MAX(수집일자) FROM src.네이버부동산_매물)
        """
        )

        # 2. 단지별 매물 카운트 뷰
        print("Creating v_naver_maemul_count...")
        con.execute(
            """
            CREATE OR REPLACE VIEW v_naver_maemul_count AS
            SELECT 
                지번주소,
                단지명,
                거래유형,
                COUNT(*) AS 매물수,
                SUM(CASE WHEN 급매_태그 = '급매' THEN 1 ELSE 0 END) AS 급매건수,
                ROUND(SUM(CASE WHEN 급매_태그 = '급매' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS 급매비중
            FROM v_naver_maemul_snapshot
            GROUP BY 1, 2, 3
        """
        )

        # 3. 호가-실거래 비교 뷰 (매매 기준, 급매 판정 규칙 포함)
        # 평당매매가격_중앙값 바탕으로 추정한 실거래가와 호가를 비교하여 판정
        print("Creating v_naver_hoga_vs_actual...")
        con.execute(
            """
            CREATE OR REPLACE VIEW v_naver_hoga_vs_actual AS
            SELECT 
                a.지번주소,
                a.단지명,
                a.매물번호,
                a.가격_만원 AS 호가,
                a.급매_태그,
                b.직전12개월_평당매매가격_중앙값 * (a.전용면적 / 3.305785) AS 직전거래추정가,
                CASE 
                    WHEN a.가격_만원 < (b.직전12개월_평당매매가격_중앙값 * (a.전용면적 / 3.305785)) * 0.9 THEN '강한 가격기준 급매'
                    WHEN a.가격_만원 <= (b.직전12개월_평당매매가격_중앙값 * (a.전용면적 / 3.305785)) THEN '가격기준 급매'
                    WHEN a.급매_태그 = '급매' THEN '태그만 급매'
                    ELSE '일반매물'
                END AS 실질급매_판정
            FROM v_naver_maemul_snapshot a
            LEFT JOIN t_class_ranking_base b 
                ON a.지번주소 = b.지번주소 
            WHERE a.거래유형 = '매매'
        """
        )

        print("Naver views created successfully.")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
