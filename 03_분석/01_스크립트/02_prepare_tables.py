import duckdb
import os


def main():
    # Because apartment.duckdb is used by another tool/process, we cannot open in write mode.
    # We create a new local DB '02_데이터/03_가공/analysis.duckdb' and attach apartment.duckdb as READ_ONLY.
    db_path = "02_데이터/03_가공/analysis.duckdb"
    con = duckdb.connect(db_path)

    source_db = "/Volumes/T9/duckdb-analytics/db/apartment.duckdb"
    con.execute(f"ATTACH '{source_db}' AS src (READ_ONLY)")

    # 1. v_sale_clean: 매매 데이터 클렌징 및 파생 컬럼 생성
    print("Creating v_sale_clean view...")
    con.execute(
        """
        CREATE OR REPLACE VIEW v_sale_clean AS
        SELECT 
            *,
            CASE 
                WHEN "전용면적(㎡)" <= 40 THEN '초소형'
                WHEN "전용면적(㎡)" <= 60 THEN '소형'
                WHEN "전용면적(㎡)" <= 85 THEN '중소형'
                WHEN "전용면적(㎡)" <= 135 THEN '중대형'
                ELSE '대형'
            END AS 전용면적_구분,
            "전용면적(㎡)" * 0.4 AS 추정평형,
            CASE 
                WHEN "전용면적(㎡)" * 0.4 < 10 THEN '10평미만'
                WHEN "전용면적(㎡)" * 0.4 < 20 THEN '10평대'
                WHEN "전용면적(㎡)" * 0.4 < 30 THEN '20평대'
                WHEN "전용면적(㎡)" * 0.4 < 40 THEN '30평대'
                WHEN "전용면적(㎡)" * 0.4 < 50 THEN '40평대'
                WHEN "전용면적(㎡)" * 0.4 < 60 THEN '50평대'
                ELSE '60평이상'
            END AS 평형대_구분,
            CASE WHEN 시도 = '세종특별자치시' THEN '세종시' ELSE 시군구 END AS 파싱_시군구,
            CAST(계약년월 / 100 AS INTEGER) AS 계약연도,
            2026 - 건축년도 AS 연식,
            CASE 
                WHEN 2026 - 건축년도 < 5 THEN '5년미만'
                WHEN 2026 - 건축년도 < 10 THEN '5~10년미만'
                WHEN 2026 - 건축년도 < 20 THEN '10~20년미만'
                WHEN 2026 - 건축년도 < 30 THEN '20~30년미만'
                ELSE '30년이상'
            END AS 연식_구분,
            CONCAT_WS(' ', 시도, 시군구, 읍면동, 리, 번지) AS 지번주소,
            REPLACE(단지명, ' ', '') AS 단지명_공백제거,
            CAST(REPLACE("거래금액(만원)", ',', '') AS BIGINT) AS 거래금액_숫자
        FROM src.매매
        WHERE "거래금액(만원)" IS NOT NULL 
          AND "전용면적(㎡)" > 0
          AND CAST(REPLACE("거래금액(만원)", ',', '') AS BIGINT) > 0
          AND (해제사유발생일 IS NULL OR 해제사유발생일 = '')
          AND (거래유형 IS NULL OR 거래유형 != '직거래')
    """
    )

    # 2. v_rent_clean: 전월세 데이터 클렌징 및 파생 컬럼
    print("Creating v_rent_clean view...")
    con.execute(
        """
        CREATE OR REPLACE VIEW v_rent_clean AS
        SELECT 
            *,
            CASE 
                WHEN "전용면적(㎡)" <= 40 THEN '초소형'
                WHEN "전용면적(㎡)" <= 60 THEN '소형'
                WHEN "전용면적(㎡)" <= 85 THEN '중소형'
                WHEN "전용면적(㎡)" <= 135 THEN '중대형'
                ELSE '대형'
            END AS 전용면적_구분,
            "전용면적(㎡)" * 0.4 AS 추정평형,
            CASE 
                WHEN "전용면적(㎡)" * 0.4 < 10 THEN '10평미만'
                WHEN "전용면적(㎡)" * 0.4 < 20 THEN '10평대'
                WHEN "전용면적(㎡)" * 0.4 < 30 THEN '20평대'
                WHEN "전용면적(㎡)" * 0.4 < 40 THEN '30평대'
                WHEN "전용면적(㎡)" * 0.4 < 50 THEN '40평대'
                WHEN "전용면적(㎡)" * 0.4 < 60 THEN '50평대'
                ELSE '60평이상'
            END AS 평형대_구분,
            CASE WHEN 시도 = '세종특별자치시' THEN '세종시' ELSE 시군구 END AS 파싱_시군구,
            CAST(계약년월 / 100 AS INTEGER) AS 계약연도,
            2026 - 건축년도 AS 연식,
            CONCAT_WS(' ', 시도, 시군구, 읍면동, 리, 번지) AS 지번주소,
            REPLACE(단지명, ' ', '') AS 단지명_공백제거,
            CAST(REPLACE(IFNULL("보증금(만원)", '0'), ',', '') AS BIGINT) AS 보증금_숫자,
            CAST(REPLACE(IFNULL("월세금(만원)", '0'), ',', '') AS BIGINT) AS 월세금_숫자
        FROM src.전월세
        WHERE "전용면적(㎡)" > 0
    """
    )

    # 3. v_sale_monthly_px: 매매 대표 가격
    print("Creating t_sale_monthly_px table...")
    con.execute(
        """
        CREATE OR REPLACE TABLE t_sale_monthly_px AS
        SELECT 
            지번주소, 단지명_공백제거 AS 단지명, 전용면적_구분, 계약년월,
            MEDIAN(거래금액_숫자) AS 매매대표가격,
            MEDIAN(거래금액_숫자 / ("전용면적(㎡)" / 3.305785)) AS 평당매매가격,
            COUNT(*) AS 매매거래량
        FROM v_sale_clean
        GROUP BY 1, 2, 3, 4
    """
    )

    # 4. v_jeonse_monthly_px: 전세 대표 가격
    print("Creating t_jeonse_monthly_px table...")
    con.execute(
        """
        CREATE OR REPLACE TABLE t_jeonse_monthly_px AS
        SELECT 
            지번주소, 단지명_공백제거 AS 단지명, 전용면적_구분, 계약년월,
            MEDIAN(보증금_숫자) AS 전세보증금대표,
            COUNT(*) AS 전세거래량
        FROM v_rent_clean
        WHERE 전월세구분 = '전세'
        GROUP BY 1, 2, 3, 4
    """
    )

    # 5. v_wolse_monthly_px: 월세 대표 월세액 및 보증금
    print("Creating t_wolse_monthly_px table...")
    con.execute(
        """
        CREATE OR REPLACE TABLE t_wolse_monthly_px AS
        SELECT 
            지번주소, 단지명_공백제거 AS 단지명, 전용면적_구분, 계약년월,
            MEDIAN(보증금_숫자) AS 월세보증금대표,
            MEDIAN(월세금_숫자) AS 월세금대표,
            COUNT(*) AS 월세거래량
        FROM v_rent_clean
        WHERE 전월세구분 = '월세'
        GROUP BY 1, 2, 3, 4
    """
    )

    # 6. 신규 준공 공급 프록시 (건축년도 + 세대수 기준 연단위 추산)
    print("Creating t_supply_proxy_annual table...")
    con.execute(
        """
        CREATE OR REPLACE TABLE t_supply_proxy_annual AS
        SELECT 
            SUBSTRING(주소, 1, INSTR(주소, ' ')-1) AS 시도,
            건축년도,
            SUM(세대수) AS 신규공급_프록시
        FROM (
            SELECT 주소, CAST(SUBSTRING(사용승인일, 1, 4) AS INTEGER) AS 건축년도, 세대수
            FROM src.공동주택_전국
            WHERE 단지구분코드 = '1' AND 사용승인일 IS NOT NULL AND 사용승인일 != '' AND LENGTH(사용승인일) >= 4
        )
        GROUP BY 1, 2
    """
    )

    # 7. 급지 분류용 기준 가격 테이블 (직전 12개월: 202504~202603 기준)
    print("Creating t_class_ranking_base table...")
    con.execute(
        """
        CREATE OR REPLACE TABLE t_class_ranking_base AS
        SELECT 
            파싱_시군구 AS 시군구, 
            지번주소, 단지명_공백제거 AS 단지명, 전용면적_구분,
            MEDIAN(거래금액_숫자 / ("전용면적(㎡)" / 3.305785)) AS 직전12개월_평당매매가격_중앙값,
            COUNT(*) AS 직전12개월_거래량
        FROM v_sale_clean
        WHERE 계약년월 BETWEEN 202504 AND 202603
        GROUP BY 1, 2, 3, 4
    """
    )

    print(
        "Data preparation views and tables created successfully in 02_데이터/03_가공/analysis.duckdb"
    )


if __name__ == "__main__":
    main()
