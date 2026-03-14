from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
DB_PATH = Path("/Volumes/T9/duckdb-analytics/db/apartment.duckdb")
OUT_DIR = ROOT / "04_결과" / "01_리포트_codex" / "01_매매시장"

BUYER_RESIDENCE_OUT = OUT_DIR / "01_매매시장_매입자거주지구성_20260314_codex.csv"
TRADE_SIZE_OUT = OUT_DIR / "01_매매시장_거래규모구성_20260314_codex.csv"
TRADE_PARTY_OUT = OUT_DIR / "01_매매시장_거래주체구성_20260314_codex.csv"
BUYER_AGE_OUT = OUT_DIR / "01_매매시장_매입자연령대구성_20260314_codex.csv"


BUYER_RESIDENCE_SQL = """
WITH base AS (
    SELECT
        시도,
        시군구,
        strptime(시점, '%Y.%m') AS dt,
        SUM(호수) AS total_units,
        SUM(CASE WHEN 매입자거주지 = '관할 시군구내' THEN 호수 ELSE 0 END) AS local_units,
        SUM(CASE WHEN 매입자거주지 = '관할 시도내' THEN 호수 ELSE 0 END) AS same_sido_units,
        SUM(CASE WHEN 매입자거주지 = '서울' THEN 호수 ELSE 0 END) AS seoul_external_units,
        SUM(CASE WHEN 매입자거주지 NOT IN ('관할 시군구내', '관할 시도내', '서울') THEN 호수 ELSE 0 END) AS other_external_units
    FROM KOSIS_아파트매매_매입자거주지별
    GROUP BY 1, 2, 3
),
latest AS (
    SELECT MAX(dt) AS latest_dt FROM base
),
agg AS (
    SELECT
        b.시도,
        b.시군구,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 2 MONTH AND l.latest_dt THEN b.total_units ELSE 0 END) AS total_recent_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 5 MONTH AND l.latest_dt THEN b.total_units ELSE 0 END) AS total_recent_6m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 14 MONTH AND l.latest_dt - INTERVAL 12 MONTH THEN b.total_units ELSE 0 END) AS total_prev_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 2 MONTH AND l.latest_dt THEN b.local_units ELSE 0 END) AS local_recent_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 5 MONTH AND l.latest_dt THEN b.local_units ELSE 0 END) AS local_recent_6m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 14 MONTH AND l.latest_dt - INTERVAL 12 MONTH THEN b.local_units ELSE 0 END) AS local_prev_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 2 MONTH AND l.latest_dt THEN b.same_sido_units ELSE 0 END) AS same_sido_recent_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 5 MONTH AND l.latest_dt THEN b.same_sido_units ELSE 0 END) AS same_sido_recent_6m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 14 MONTH AND l.latest_dt - INTERVAL 12 MONTH THEN b.same_sido_units ELSE 0 END) AS same_sido_prev_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 2 MONTH AND l.latest_dt THEN b.seoul_external_units ELSE 0 END) AS seoul_external_recent_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 5 MONTH AND l.latest_dt THEN b.seoul_external_units ELSE 0 END) AS seoul_external_recent_6m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 14 MONTH AND l.latest_dt - INTERVAL 12 MONTH THEN b.seoul_external_units ELSE 0 END) AS seoul_external_prev_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 2 MONTH AND l.latest_dt THEN b.other_external_units ELSE 0 END) AS other_external_recent_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 5 MONTH AND l.latest_dt THEN b.other_external_units ELSE 0 END) AS other_external_recent_6m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 14 MONTH AND l.latest_dt - INTERVAL 12 MONTH THEN b.other_external_units ELSE 0 END) AS other_external_prev_3m
    FROM base b
    CROSS JOIN latest l
    GROUP BY 1, 2
)
SELECT
    시도,
    시군구,
    total_recent_3m,
    total_recent_6m,
    ROUND(100.0 * local_recent_3m / NULLIF(total_recent_3m, 0), 1) AS 시군구내비중_최근3m,
    ROUND(100.0 * same_sido_recent_3m / NULLIF(total_recent_3m, 0), 1) AS 시도내비중_최근3m,
    ROUND(100.0 * seoul_external_recent_3m / NULLIF(total_recent_3m, 0), 1) AS 서울외지비중_최근3m,
    ROUND(100.0 * other_external_recent_3m / NULLIF(total_recent_3m, 0), 1) AS 기타외지비중_최근3m,
    ROUND(100.0 * local_recent_6m / NULLIF(total_recent_6m, 0), 1) AS 시군구내비중_최근6m,
    ROUND(100.0 * same_sido_recent_6m / NULLIF(total_recent_6m, 0), 1) AS 시도내비중_최근6m,
    ROUND(100.0 * seoul_external_recent_6m / NULLIF(total_recent_6m, 0), 1) AS 서울외지비중_최근6m,
    ROUND(100.0 * other_external_recent_6m / NULLIF(total_recent_6m, 0), 1) AS 기타외지비중_최근6m,
    ROUND(100.0 * local_recent_3m / NULLIF(total_recent_3m, 0), 1) - ROUND(100.0 * local_prev_3m / NULLIF(total_prev_3m, 0), 1) AS 시군구내비중_변화_3mYoY,
    ROUND(100.0 * same_sido_recent_3m / NULLIF(total_recent_3m, 0), 1) - ROUND(100.0 * same_sido_prev_3m / NULLIF(total_prev_3m, 0), 1) AS 시도내비중_변화_3mYoY,
    ROUND(100.0 * seoul_external_recent_3m / NULLIF(total_recent_3m, 0), 1) - ROUND(100.0 * seoul_external_prev_3m / NULLIF(total_prev_3m, 0), 1) AS 서울외지비중_변화_3mYoY,
    ROUND(100.0 * other_external_recent_3m / NULLIF(total_recent_3m, 0), 1) - ROUND(100.0 * other_external_prev_3m / NULLIF(total_prev_3m, 0), 1) AS 기타외지비중_변화_3mYoY
FROM agg
WHERE total_recent_3m >= 30
ORDER BY total_recent_3m DESC, 시도, 시군구
"""


TRADE_SIZE_SQL = """
WITH base AS (
    SELECT
        시도,
        시군구,
        strptime(시점, '%Y.%m') AS dt,
        SUM(호수) AS total_units,
        SUM(CASE WHEN 거래규모 IN ('20㎡이하', '21~40㎡', '41~60㎡') THEN 호수 ELSE 0 END) AS small_units,
        SUM(CASE WHEN 거래규모 = '61~85㎡' THEN 호수 ELSE 0 END) AS national_standard_units,
        SUM(CASE WHEN 거래규모 IN ('86~100㎡', '101~135㎡') THEN 호수 ELSE 0 END) AS mid_large_units,
        SUM(CASE WHEN 거래규모 IN ('136~165㎡', '166~198㎡', '198㎡초과') THEN 호수 ELSE 0 END) AS large_units
    FROM KOSIS_아파트매매_거래규모별
    GROUP BY 1, 2, 3
),
latest AS (
    SELECT MAX(dt) AS latest_dt FROM base
),
agg AS (
    SELECT
        b.시도,
        b.시군구,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 2 MONTH AND l.latest_dt THEN b.total_units ELSE 0 END) AS total_recent_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 5 MONTH AND l.latest_dt THEN b.total_units ELSE 0 END) AS total_recent_6m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 14 MONTH AND l.latest_dt - INTERVAL 12 MONTH THEN b.total_units ELSE 0 END) AS total_prev_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 2 MONTH AND l.latest_dt THEN b.small_units ELSE 0 END) AS small_recent_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 5 MONTH AND l.latest_dt THEN b.small_units ELSE 0 END) AS small_recent_6m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 14 MONTH AND l.latest_dt - INTERVAL 12 MONTH THEN b.small_units ELSE 0 END) AS small_prev_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 2 MONTH AND l.latest_dt THEN b.national_standard_units ELSE 0 END) AS standard_recent_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 5 MONTH AND l.latest_dt THEN b.national_standard_units ELSE 0 END) AS standard_recent_6m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 14 MONTH AND l.latest_dt - INTERVAL 12 MONTH THEN b.national_standard_units ELSE 0 END) AS standard_prev_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 2 MONTH AND l.latest_dt THEN b.mid_large_units ELSE 0 END) AS mid_large_recent_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 5 MONTH AND l.latest_dt THEN b.mid_large_units ELSE 0 END) AS mid_large_recent_6m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 14 MONTH AND l.latest_dt - INTERVAL 12 MONTH THEN b.mid_large_units ELSE 0 END) AS mid_large_prev_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 2 MONTH AND l.latest_dt THEN b.large_units ELSE 0 END) AS large_recent_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 5 MONTH AND l.latest_dt THEN b.large_units ELSE 0 END) AS large_recent_6m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 14 MONTH AND l.latest_dt - INTERVAL 12 MONTH THEN b.large_units ELSE 0 END) AS large_prev_3m
    FROM base b
    CROSS JOIN latest l
    GROUP BY 1, 2
)
SELECT
    시도,
    시군구,
    total_recent_3m,
    total_recent_6m,
    ROUND(100.0 * small_recent_3m / NULLIF(total_recent_3m, 0), 1) AS 소형비중_최근3m,
    ROUND(100.0 * standard_recent_3m / NULLIF(total_recent_3m, 0), 1) AS 국민평형비중_최근3m,
    ROUND(100.0 * mid_large_recent_3m / NULLIF(total_recent_3m, 0), 1) AS 중대형비중_최근3m,
    ROUND(100.0 * large_recent_3m / NULLIF(total_recent_3m, 0), 1) AS 대형비중_최근3m,
    ROUND(100.0 * small_recent_6m / NULLIF(total_recent_6m, 0), 1) AS 소형비중_최근6m,
    ROUND(100.0 * standard_recent_6m / NULLIF(total_recent_6m, 0), 1) AS 국민평형비중_최근6m,
    ROUND(100.0 * mid_large_recent_6m / NULLIF(total_recent_6m, 0), 1) AS 중대형비중_최근6m,
    ROUND(100.0 * large_recent_6m / NULLIF(total_recent_6m, 0), 1) AS 대형비중_최근6m,
    ROUND(100.0 * small_recent_3m / NULLIF(total_recent_3m, 0), 1) - ROUND(100.0 * small_prev_3m / NULLIF(total_prev_3m, 0), 1) AS 소형비중_변화_3mYoY,
    ROUND(100.0 * standard_recent_3m / NULLIF(total_recent_3m, 0), 1) - ROUND(100.0 * standard_prev_3m / NULLIF(total_prev_3m, 0), 1) AS 국민평형비중_변화_3mYoY,
    ROUND(100.0 * mid_large_recent_3m / NULLIF(total_recent_3m, 0), 1) - ROUND(100.0 * mid_large_prev_3m / NULLIF(total_prev_3m, 0), 1) AS 중대형비중_변화_3mYoY,
    ROUND(100.0 * large_recent_3m / NULLIF(total_recent_3m, 0), 1) - ROUND(100.0 * large_prev_3m / NULLIF(total_prev_3m, 0), 1) AS 대형비중_변화_3mYoY
FROM agg
WHERE total_recent_3m >= 30
ORDER BY total_recent_3m DESC, 시도, 시군구
"""


TRADE_PARTY_SQL = """
WITH base AS (
    SELECT
        시도,
        시군구,
        strptime(시점, '%Y.%m') AS dt,
        SUM(호수) AS total_units,
        SUM(CASE WHEN 거래주체 = '개인-＞개인' THEN 호수 ELSE 0 END) AS individual_to_individual_units,
        SUM(CASE WHEN 거래주체 IN ('개인-＞개인', '법인-＞개인', '기타-＞개인') THEN 호수 ELSE 0 END) AS individual_buyer_units,
        SUM(CASE WHEN 거래주체 IN ('개인-＞법인', '법인-＞법인', '기타-＞법인') THEN 호수 ELSE 0 END) AS corporate_buyer_units,
        SUM(CASE WHEN 거래주체 IN ('개인-＞기타', '법인-＞기타', '기타-＞기타') THEN 호수 ELSE 0 END) AS other_buyer_units,
        SUM(CASE WHEN 거래주체 IN ('법인-＞개인', '기타-＞개인') THEN 호수 ELSE 0 END) AS non_individual_to_individual_units
    FROM KOSIS_아파트매매_거래주체별
    GROUP BY 1, 2, 3
),
latest AS (
    SELECT MAX(dt) AS latest_dt FROM base
),
agg AS (
    SELECT
        b.시도,
        b.시군구,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 2 MONTH AND l.latest_dt THEN b.total_units ELSE 0 END) AS total_recent_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 5 MONTH AND l.latest_dt THEN b.total_units ELSE 0 END) AS total_recent_6m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 14 MONTH AND l.latest_dt - INTERVAL 12 MONTH THEN b.total_units ELSE 0 END) AS total_prev_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 2 MONTH AND l.latest_dt THEN b.individual_to_individual_units ELSE 0 END) AS individual_to_individual_recent_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 5 MONTH AND l.latest_dt THEN b.individual_to_individual_units ELSE 0 END) AS individual_to_individual_recent_6m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 14 MONTH AND l.latest_dt - INTERVAL 12 MONTH THEN b.individual_to_individual_units ELSE 0 END) AS individual_to_individual_prev_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 2 MONTH AND l.latest_dt THEN b.individual_buyer_units ELSE 0 END) AS individual_buyer_recent_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 5 MONTH AND l.latest_dt THEN b.individual_buyer_units ELSE 0 END) AS individual_buyer_recent_6m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 14 MONTH AND l.latest_dt - INTERVAL 12 MONTH THEN b.individual_buyer_units ELSE 0 END) AS individual_buyer_prev_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 2 MONTH AND l.latest_dt THEN b.corporate_buyer_units ELSE 0 END) AS corporate_buyer_recent_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 5 MONTH AND l.latest_dt THEN b.corporate_buyer_units ELSE 0 END) AS corporate_buyer_recent_6m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 14 MONTH AND l.latest_dt - INTERVAL 12 MONTH THEN b.corporate_buyer_units ELSE 0 END) AS corporate_buyer_prev_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 2 MONTH AND l.latest_dt THEN b.other_buyer_units ELSE 0 END) AS other_buyer_recent_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 5 MONTH AND l.latest_dt THEN b.other_buyer_units ELSE 0 END) AS other_buyer_recent_6m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 14 MONTH AND l.latest_dt - INTERVAL 12 MONTH THEN b.other_buyer_units ELSE 0 END) AS other_buyer_prev_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 2 MONTH AND l.latest_dt THEN b.non_individual_to_individual_units ELSE 0 END) AS non_individual_to_individual_recent_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 5 MONTH AND l.latest_dt THEN b.non_individual_to_individual_units ELSE 0 END) AS non_individual_to_individual_recent_6m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 14 MONTH AND l.latest_dt - INTERVAL 12 MONTH THEN b.non_individual_to_individual_units ELSE 0 END) AS non_individual_to_individual_prev_3m
    FROM base b
    CROSS JOIN latest l
    GROUP BY 1, 2
)
SELECT
    시도,
    시군구,
    total_recent_3m,
    total_recent_6m,
    ROUND(100.0 * individual_to_individual_recent_3m / NULLIF(total_recent_3m, 0), 1) AS 개인간거래비중_최근3m,
    ROUND(100.0 * individual_buyer_recent_3m / NULLIF(total_recent_3m, 0), 1) AS 개인매수비중_최근3m,
    ROUND(100.0 * corporate_buyer_recent_3m / NULLIF(total_recent_3m, 0), 1) AS 법인매수비중_최근3m,
    ROUND(100.0 * other_buyer_recent_3m / NULLIF(total_recent_3m, 0), 1) AS 기타매수비중_최근3m,
    ROUND(100.0 * non_individual_to_individual_recent_3m / NULLIF(total_recent_3m, 0), 1) AS 비개인매도물량_개인흡수비중_최근3m,
    ROUND(100.0 * individual_to_individual_recent_6m / NULLIF(total_recent_6m, 0), 1) AS 개인간거래비중_최근6m,
    ROUND(100.0 * individual_buyer_recent_6m / NULLIF(total_recent_6m, 0), 1) AS 개인매수비중_최근6m,
    ROUND(100.0 * corporate_buyer_recent_6m / NULLIF(total_recent_6m, 0), 1) AS 법인매수비중_최근6m,
    ROUND(100.0 * other_buyer_recent_6m / NULLIF(total_recent_6m, 0), 1) AS 기타매수비중_최근6m,
    ROUND(100.0 * non_individual_to_individual_recent_6m / NULLIF(total_recent_6m, 0), 1) AS 비개인매도물량_개인흡수비중_최근6m,
    ROUND(100.0 * individual_to_individual_recent_3m / NULLIF(total_recent_3m, 0), 1) - ROUND(100.0 * individual_to_individual_prev_3m / NULLIF(total_prev_3m, 0), 1) AS 개인간거래비중_변화_3mYoY,
    ROUND(100.0 * individual_buyer_recent_3m / NULLIF(total_recent_3m, 0), 1) - ROUND(100.0 * individual_buyer_prev_3m / NULLIF(total_prev_3m, 0), 1) AS 개인매수비중_변화_3mYoY,
    ROUND(100.0 * corporate_buyer_recent_3m / NULLIF(total_recent_3m, 0), 1) - ROUND(100.0 * corporate_buyer_prev_3m / NULLIF(total_prev_3m, 0), 1) AS 법인매수비중_변화_3mYoY,
    ROUND(100.0 * other_buyer_recent_3m / NULLIF(total_recent_3m, 0), 1) - ROUND(100.0 * other_buyer_prev_3m / NULLIF(total_prev_3m, 0), 1) AS 기타매수비중_변화_3mYoY,
    ROUND(100.0 * non_individual_to_individual_recent_3m / NULLIF(total_recent_3m, 0), 1) - ROUND(100.0 * non_individual_to_individual_prev_3m / NULLIF(total_prev_3m, 0), 1) AS 비개인매도물량_개인흡수비중_변화_3mYoY
FROM agg
WHERE total_recent_3m >= 30
ORDER BY total_recent_3m DESC, 시도, 시군구
"""


BUYER_AGE_SQL = """
WITH base AS (
    SELECT
        시도,
        시군구,
        strptime(시점, '%Y.%m') AS dt,
        SUM(호수) AS total_units,
        SUM(CASE WHEN 매입자연령대 = '20대이하' THEN 호수 ELSE 0 END) AS under_20_units,
        SUM(CASE WHEN 매입자연령대 = '30대' THEN 호수 ELSE 0 END) AS age_30_units,
        SUM(CASE WHEN 매입자연령대 IN ('40대', '50대') THEN 호수 ELSE 0 END) AS age_40_50_units,
        SUM(CASE WHEN 매입자연령대 IN ('60대', '70대이상') THEN 호수 ELSE 0 END) AS age_60_plus_units
    FROM KOSIS_아파트매매_매입자연령대별
    GROUP BY 1, 2, 3
),
latest AS (
    SELECT MAX(dt) AS latest_dt FROM base
),
agg AS (
    SELECT
        b.시도,
        b.시군구,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 2 MONTH AND l.latest_dt THEN b.total_units ELSE 0 END) AS total_recent_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 5 MONTH AND l.latest_dt THEN b.total_units ELSE 0 END) AS total_recent_6m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 14 MONTH AND l.latest_dt - INTERVAL 12 MONTH THEN b.total_units ELSE 0 END) AS total_prev_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 2 MONTH AND l.latest_dt THEN b.under_20_units ELSE 0 END) AS under_20_recent_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 5 MONTH AND l.latest_dt THEN b.under_20_units ELSE 0 END) AS under_20_recent_6m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 14 MONTH AND l.latest_dt - INTERVAL 12 MONTH THEN b.under_20_units ELSE 0 END) AS under_20_prev_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 2 MONTH AND l.latest_dt THEN b.age_30_units ELSE 0 END) AS age_30_recent_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 5 MONTH AND l.latest_dt THEN b.age_30_units ELSE 0 END) AS age_30_recent_6m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 14 MONTH AND l.latest_dt - INTERVAL 12 MONTH THEN b.age_30_units ELSE 0 END) AS age_30_prev_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 2 MONTH AND l.latest_dt THEN b.age_40_50_units ELSE 0 END) AS age_40_50_recent_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 5 MONTH AND l.latest_dt THEN b.age_40_50_units ELSE 0 END) AS age_40_50_recent_6m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 14 MONTH AND l.latest_dt - INTERVAL 12 MONTH THEN b.age_40_50_units ELSE 0 END) AS age_40_50_prev_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 2 MONTH AND l.latest_dt THEN b.age_60_plus_units ELSE 0 END) AS age_60_plus_recent_3m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 5 MONTH AND l.latest_dt THEN b.age_60_plus_units ELSE 0 END) AS age_60_plus_recent_6m,
        SUM(CASE WHEN b.dt BETWEEN l.latest_dt - INTERVAL 14 MONTH AND l.latest_dt - INTERVAL 12 MONTH THEN b.age_60_plus_units ELSE 0 END) AS age_60_plus_prev_3m
    FROM base b
    CROSS JOIN latest l
    GROUP BY 1, 2
)
SELECT
    시도,
    시군구,
    total_recent_3m,
    total_recent_6m,
    ROUND(100.0 * under_20_recent_3m / NULLIF(total_recent_3m, 0), 1) AS age20_under_share_recent_3m,
    ROUND(100.0 * age_30_recent_3m / NULLIF(total_recent_3m, 0), 1) AS age30_share_recent_3m,
    ROUND(100.0 * age_40_50_recent_3m / NULLIF(total_recent_3m, 0), 1) AS age40_50_share_recent_3m,
    ROUND(100.0 * age_60_plus_recent_3m / NULLIF(total_recent_3m, 0), 1) AS age60_plus_share_recent_3m,
    ROUND(100.0 * (age_30_recent_3m + age_40_50_recent_3m) / NULLIF(total_recent_3m, 0), 1) AS age30_50_share_recent_3m,
    ROUND(100.0 * under_20_recent_6m / NULLIF(total_recent_6m, 0), 1) AS age20_under_share_recent_6m,
    ROUND(100.0 * age_30_recent_6m / NULLIF(total_recent_6m, 0), 1) AS age30_share_recent_6m,
    ROUND(100.0 * age_40_50_recent_6m / NULLIF(total_recent_6m, 0), 1) AS age40_50_share_recent_6m,
    ROUND(100.0 * age_60_plus_recent_6m / NULLIF(total_recent_6m, 0), 1) AS age60_plus_share_recent_6m,
    ROUND(100.0 * (age_30_recent_6m + age_40_50_recent_6m) / NULLIF(total_recent_6m, 0), 1) AS age30_50_share_recent_6m,
    ROUND(100.0 * under_20_recent_3m / NULLIF(total_recent_3m, 0), 1) - ROUND(100.0 * under_20_prev_3m / NULLIF(total_prev_3m, 0), 1) AS age20_under_share_change_3m_yoy,
    ROUND(100.0 * age_30_recent_3m / NULLIF(total_recent_3m, 0), 1) - ROUND(100.0 * age_30_prev_3m / NULLIF(total_prev_3m, 0), 1) AS age30_share_change_3m_yoy,
    ROUND(100.0 * age_40_50_recent_3m / NULLIF(total_recent_3m, 0), 1) - ROUND(100.0 * age_40_50_prev_3m / NULLIF(total_prev_3m, 0), 1) AS age40_50_share_change_3m_yoy,
    ROUND(100.0 * age_60_plus_recent_3m / NULLIF(total_recent_3m, 0), 1) - ROUND(100.0 * age_60_plus_prev_3m / NULLIF(total_prev_3m, 0), 1) AS age60_plus_share_change_3m_yoy,
    ROUND(100.0 * (age_30_recent_3m + age_40_50_recent_3m) / NULLIF(total_recent_3m, 0), 1) - ROUND(100.0 * (age_30_prev_3m + age_40_50_prev_3m) / NULLIF(total_prev_3m, 0), 1) AS age30_50_share_change_3m_yoy
FROM agg
WHERE total_recent_3m >= 30
ORDER BY total_recent_3m DESC, 시도, 시군구
"""


def write_query(con: duckdb.DuckDBPyConnection, sql: str, out_path: Path) -> pd.DataFrame:
    df = con.execute(sql).df()
    if out_path == BUYER_AGE_OUT:
        df = df.rename(
            columns={
                "age20_under_share_recent_3m": "20대이하비중_최근3m",
                "age30_share_recent_3m": "30대비중_최근3m",
                "age40_50_share_recent_3m": "40_50대비중_최근3m",
                "age60_plus_share_recent_3m": "60대이상비중_최근3m",
                "age30_50_share_recent_3m": "30_50대비중_최근3m",
                "age20_under_share_recent_6m": "20대이하비중_최근6m",
                "age30_share_recent_6m": "30대비중_최근6m",
                "age40_50_share_recent_6m": "40_50대비중_최근6m",
                "age60_plus_share_recent_6m": "60대이상비중_최근6m",
                "age30_50_share_recent_6m": "30_50대비중_최근6m",
                "age20_under_share_change_3m_yoy": "20대이하비중_변화_3mYoY",
                "age30_share_change_3m_yoy": "30대비중_변화_3mYoY",
                "age40_50_share_change_3m_yoy": "40_50대비중_변화_3mYoY",
                "age60_plus_share_change_3m_yoy": "60대이상비중_변화_3mYoY",
                "age30_50_share_change_3m_yoy": "30_50대비중_변화_3mYoY",
            }
        )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    return df


def main() -> None:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    outputs = [
        (BUYER_RESIDENCE_SQL, BUYER_RESIDENCE_OUT),
        (TRADE_SIZE_SQL, TRADE_SIZE_OUT),
        (TRADE_PARTY_SQL, TRADE_PARTY_OUT),
        (BUYER_AGE_SQL, BUYER_AGE_OUT),
    ]
    for sql, out_path in outputs:
        write_query(con, sql, out_path)
        print(f"Wrote {out_path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
