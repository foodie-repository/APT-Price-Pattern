from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
DB_PATH = Path("/Volumes/T9/duckdb-analytics/db/apartment.duckdb")
PREDICTION_CSV = (
    ROOT
    / "04_결과"
    / "01_리포트_codex"
    / "06_예측검증"
    / "06_예측점수_20260313_codex_시군구.csv"
)
OUT_DIR = ROOT / "04_결과" / "01_리포트_codex" / "00_공통"
SIDO_OUT = OUT_DIR / "00_HUG_KOSIS_KB_분양시장심리_시도요약_20260315_codex.csv"
REGION_OUT = OUT_DIR / "00_HUG_KOSIS_KB_분양시장심리_권역요약_20260315_codex.csv"


REGION_MAP = {
    "서울특별시": "수도권",
    "경기도": "수도권",
    "인천광역시": "수도권",
    "부산광역시": "지방광역시",
    "대구광역시": "지방광역시",
    "광주광역시": "지방광역시",
    "대전광역시": "지방광역시",
    "울산광역시": "지방광역시",
    "세종특별자치시": "지방광역시",
}

KB_REGION_MAP = {
    "서울": "서울특별시",
    "경기": "경기도",
    "인천": "인천광역시",
    "부산": "부산광역시",
    "대구": "대구광역시",
    "광주": "광주광역시",
    "대전": "대전광역시",
    "울산": "울산광역시",
    "세종": "세종특별자치시",
    "강원": "강원특별자치도",
    "충북": "충청북도",
    "충남": "충청남도",
    "전북": "전북특별자치도",
    "전남": "전라남도",
    "경북": "경상북도",
    "경남": "경상남도",
    "제주": "제주특별자치도",
}


def classify_primary_market(row: pd.Series) -> str:
    initial = row["초기분양률_최신"]
    idx_change = row["분양가격지수_6개월변화"]
    unit_growth = row["분양세대수_증감률pct"]

    if pd.notna(initial) and initial >= 80 and pd.notna(idx_change) and idx_change > 0:
        return "1차시장 강세"
    if pd.notna(initial) and initial >= 60 and (pd.isna(unit_growth) or unit_growth >= -20):
        return "1차시장 회복"
    if pd.notna(initial) and initial < 50 and pd.notna(idx_change) and idx_change >= 20:
        return "분양가 상승·소화 점검"
    if pd.notna(initial) and initial < 50:
        return "1차시장 부진"
    return "혼합"


def classify_sentiment(row: pd.Series) -> str:
    senti = row["주택매매심리_최근3개월"]
    sale_yoy = row["매매YoY"]
    trade_recovery = row["거래회복률"]

    if pd.notna(senti) and senti >= 120 and pd.notna(sale_yoy) and sale_yoy >= 2:
        return "심리-가격 동행 강세"
    if pd.notna(senti) and senti >= 120 and (
        pd.isna(sale_yoy) or sale_yoy < 2 or pd.isna(trade_recovery) or trade_recovery < 5
    ):
        return "심리 선행형"
    if pd.notna(senti) and senti < 100 and pd.notna(sale_yoy) and sale_yoy > 5:
        return "가격 선행형"
    if pd.notna(senti) and senti < 100:
        return "심리 약세"
    return "중립/혼합"


def classify_overall(row: pd.Series) -> str:
    primary = row["1차시장해석"]
    senti = row["심리해석"]

    if primary in {"1차시장 강세", "1차시장 회복"} and senti == "심리-가격 동행 강세":
        return "1차시장·심리 동행 회복형"
    if primary in {"1차시장 강세", "1차시장 회복"} and senti == "심리 선행형":
        return "심리 선행·정책 제약형"
    if primary == "분양가 상승·소화 점검":
        return "분양가 상승·소화 점검형"
    if primary == "1차시장 부진" and senti in {"심리 약세", "중립/혼합"}:
        return "1차시장 부진형"
    if senti == "가격 선행형":
        return "기존주택 선행형"
    return "혼합형"


def classify_kb_sentiment(row: pd.Series) -> str:
    buyer = row["KB_매수우위지수_최신"]
    outlook = row["KB_매매가격전망지수_최신"]
    active = row["KB_매매거래활발지수_최신"]

    if pd.notna(buyer) and pd.notna(outlook) and buyer >= 60 and outlook >= 105:
        return "민간심리 강세"
    if pd.notna(outlook) and outlook >= 110 and (pd.isna(buyer) or buyer < 50):
        return "전망 선행형"
    if pd.notna(active) and active >= 28 and (pd.isna(buyer) or buyer < 50):
        return "거래체감 선행형"
    if pd.notna(buyer) and buyer < 40 and pd.notna(outlook) and outlook < 100:
        return "민간심리 약세"
    return "민간심리 중립"


def load_sale_background() -> pd.DataFrame:
    pred = pd.read_csv(PREDICTION_CSV)
    pred["date"] = pd.to_datetime(pred["date"])
    latest = pred["date"].max()
    latest_df = pred[pred["date"] == latest].copy()
    agg = (
        latest_df.groupby("시도", as_index=False)
        .agg(
            기존매매평당가=("sale_pp_만원", "mean"),
            매매YoY=("price_12m_change_pct", "mean"),
            거래회복률=("trade_recovery_pct", "mean"),
        )
        .round(3)
    )
    return agg


def load_hug_sentiment() -> pd.DataFrame:
    con = duckdb.connect(DB_PATH, read_only=True)
    sale_price = con.execute(
        """
        WITH b AS (
            SELECT strptime(시점, '%Y.%m') AS dt, 시도, 분양가격
            FROM KOSIS_HUG_분양가격
            WHERE 규모 = '전체'
        ),
        l AS (SELECT MAX(dt) AS latest_dt FROM b)
        SELECT
            시도,
            strftime(MAX(CASE WHEN dt = latest_dt THEN dt END), '%Y-%m') AS 분양가격_기준월,
            ROUND(MAX(CASE WHEN dt = latest_dt THEN 분양가격 END), 1) AS 분양가격_최신
        FROM b, l
        GROUP BY 시도
        """
    ).df()
    price_index = con.execute(
        """
        WITH b AS (
            SELECT strptime(시점, '%Y.%m') AS dt, 시도, 분양가격지수
            FROM KOSIS_HUG_분양가격지수
            WHERE 규모 = '전체'
        ),
        l AS (SELECT MAX(dt) AS latest_dt FROM b)
        SELECT
            시도,
            ROUND(MAX(CASE WHEN dt = latest_dt THEN 분양가격지수 END), 1) AS 분양가격지수_최신,
            ROUND(
                MAX(CASE WHEN dt = latest_dt THEN 분양가격지수 END)
                - MAX(CASE WHEN dt = latest_dt - INTERVAL 6 MONTH THEN 분양가격지수 END),
                1
            ) AS 분양가격지수_6개월변화
        FROM b, l
        GROUP BY 시도
        """
    ).df()
    initial_sales = con.execute(
        """
        WITH b AS (
            SELECT try_strptime(replace(시점, 'Q', ''), '%Y.%m') AS dt, 시도, 초기분양률
            FROM KOSIS_HUG_초기분양률
        ),
        l AS (SELECT MAX(dt) AS latest_dt FROM b)
        SELECT
            시도,
            strftime(MAX(CASE WHEN dt = latest_dt THEN dt END), '%Y-%m') AS 초기분양률_기준월,
            ROUND(MAX(CASE WHEN dt = latest_dt THEN 초기분양률 END), 1) AS 초기분양률_최신,
            ROUND(
                MAX(CASE WHEN dt = latest_dt THEN 초기분양률 END)
                - MAX(CASE WHEN dt = latest_dt - INTERVAL 12 MONTH THEN 초기분양률 END),
                1
            ) AS 초기분양률_전년동기차
        FROM b, l
        GROUP BY 시도
        """
    ).df()
    sale_units = con.execute(
        """
        WITH b AS (
            SELECT strptime(시점, '%Y.%m') AS dt, 시도, 분양세대수
            FROM KOSIS_HUG_분양세대수
        ),
        l AS (SELECT MAX(dt) AS latest_dt FROM b)
        SELECT
            시도,
            ROUND(
                SUM(CASE WHEN dt BETWEEN latest_dt - INTERVAL 5 MONTH AND latest_dt THEN 분양세대수 ELSE 0 END),
                0
            ) AS 분양세대수_최근6개월,
            ROUND(
                100.0 * (
                    SUM(CASE WHEN dt BETWEEN latest_dt - INTERVAL 5 MONTH AND latest_dt THEN 분양세대수 ELSE 0 END)
                    - SUM(CASE WHEN dt BETWEEN latest_dt - INTERVAL 17 MONTH AND latest_dt - INTERVAL 12 MONTH THEN 분양세대수 ELSE 0 END)
                ) / NULLIF(
                    SUM(CASE WHEN dt BETWEEN latest_dt - INTERVAL 17 MONTH AND latest_dt - INTERVAL 12 MONTH THEN 분양세대수 ELSE 0 END),
                    0
                ),
                1
            ) AS 분양세대수_증감률pct
        FROM b, l
        GROUP BY 시도
        """
    ).df()
    land_ratio = con.execute(
        """
        WITH b AS (
            SELECT strptime(시점, '%Y.%m') AS dt, 시도, 대지비비율
            FROM KOSIS_HUG_대지비비율
        ),
        l AS (SELECT MAX(dt) AS latest_dt FROM b)
        SELECT
            시도,
            strftime(MAX(CASE WHEN dt = latest_dt THEN dt END), '%Y-%m') AS 대지비비율_기준월,
            ROUND(MAX(CASE WHEN dt = latest_dt THEN 대지비비율 END), 1) AS 대지비비율_최신
        FROM b, l
        GROUP BY 시도
        """
    ).df()
    sale_sentiment = con.execute(
        """
        WITH b AS (
            SELECT strptime(시점, '%Y.%m') AS dt, 시도, 주택매매시장소비심리지수
            FROM KOSIS_소비심리_주택매매
        ),
        l AS (SELECT MAX(dt) AS latest_dt FROM b)
        SELECT
            시도,
            strftime(MAX(CASE WHEN dt = latest_dt THEN dt END), '%Y-%m') AS 주택매매심리_기준월,
            ROUND(MAX(CASE WHEN dt = latest_dt THEN 주택매매시장소비심리지수 END), 1) AS 주택매매심리_최신,
            ROUND(
                AVG(CASE WHEN dt BETWEEN latest_dt - INTERVAL 2 MONTH AND latest_dt THEN 주택매매시장소비심리지수 END),
                1
            ) AS 주택매매심리_최근3개월,
            ROUND(
                AVG(CASE WHEN dt BETWEEN latest_dt - INTERVAL 2 MONTH AND latest_dt THEN 주택매매시장소비심리지수 END)
                - AVG(CASE WHEN dt BETWEEN latest_dt - INTERVAL 14 MONTH AND latest_dt - INTERVAL 12 MONTH THEN 주택매매시장소비심리지수 END),
                1
            ) AS 주택매매심리_전년동기차
        FROM b, l
        GROUP BY 시도
        """
    ).df()
    real_estate_sentiment = con.execute(
        """
        WITH b AS (
            SELECT strptime(시점, '%Y.%m') AS dt, 시도, 부동산시장소비심리지수
            FROM KOSIS_소비심리_부동산시장
        ),
        l AS (SELECT MAX(dt) AS latest_dt FROM b)
        SELECT
            시도,
            ROUND(MAX(CASE WHEN dt = latest_dt THEN 부동산시장소비심리지수 END), 1) AS 부동산시장심리_최신
        FROM b, l
        GROUP BY 시도
        """
    ).df()

    merged = sale_price
    for frame in (
        price_index,
        initial_sales,
        sale_units,
        land_ratio,
        sale_sentiment,
        real_estate_sentiment,
    ):
        merged = merged.merge(frame, on="시도", how="outer")
    return merged


def load_kb_sentiment() -> pd.DataFrame:
    con = duckdb.connect(DB_PATH, read_only=True)
    buyer = con.execute(
        """
        WITH b AS (
            SELECT 날짜, 지역명, 매수우위지수
            FROM KB_매수우위지수
            WHERE 월간주간구분 = '월간'
        ),
        l AS (SELECT MAX(날짜) AS latest_dt FROM b)
        SELECT
            지역명,
            strftime(MAX(CASE WHEN 날짜 = latest_dt THEN 날짜 END), '%Y-%m') AS KB_기준월,
            ROUND(MAX(CASE WHEN 날짜 = latest_dt THEN 매수우위지수 END), 1) AS KB_매수우위지수_최신,
            ROUND(
                AVG(CASE WHEN 날짜 BETWEEN latest_dt - INTERVAL 2 MONTH AND latest_dt THEN 매수우위지수 END),
                1
            ) AS KB_매수우위지수_최근3개월
        FROM b, l
        GROUP BY 지역명
        """
    ).df()
    outlook = con.execute(
        """
        WITH b AS (
            SELECT 날짜, 지역명, 매매상승하락전망지수
            FROM KB_매매가격전망지수
            WHERE 월간주간구분 = '월간'
        ),
        l AS (SELECT MAX(날짜) AS latest_dt FROM b)
        SELECT
            지역명,
            ROUND(MAX(CASE WHEN 날짜 = latest_dt THEN 매매상승하락전망지수 END), 1) AS KB_매매가격전망지수_최신,
            ROUND(
                AVG(CASE WHEN 날짜 BETWEEN latest_dt - INTERVAL 2 MONTH AND latest_dt THEN 매매상승하락전망지수 END),
                1
            ) AS KB_매매가격전망지수_최근3개월
        FROM b, l
        GROUP BY 지역명
        """
    ).df()
    active = con.execute(
        """
        WITH b AS (
            SELECT 날짜, 지역명, 매매거래지수
            FROM KB_매매거래활발지수
            WHERE 월간주간구분 = '월간'
        ),
        l AS (SELECT MAX(날짜) AS latest_dt FROM b)
        SELECT
            지역명,
            ROUND(MAX(CASE WHEN 날짜 = latest_dt THEN 매매거래지수 END), 1) AS KB_매매거래활발지수_최신,
            ROUND(
                AVG(CASE WHEN 날짜 BETWEEN latest_dt - INTERVAL 2 MONTH AND latest_dt THEN 매매거래지수 END),
                1
            ) AS KB_매매거래활발지수_최근3개월
        FROM b, l
        GROUP BY 지역명
        """
    ).df()
    merged = buyer.merge(outlook, on="지역명", how="outer").merge(active, on="지역명", how="outer")
    merged["시도"] = merged["지역명"].map(KB_REGION_MAP)
    merged = merged[merged["시도"].notna()].drop(columns=["지역명"])
    return merged


def main() -> None:
    sale = load_sale_background()
    hug = load_hug_sentiment()
    kb = load_kb_sentiment()
    merged = sale.merge(hug, on="시도", how="outer").merge(kb, on="시도", how="left")
    merged["권역"] = merged["시도"].map(REGION_MAP).fillna("기타지방")
    merged["분양가프리미엄_pct"] = (
        (merged["분양가격_최신"] - merged["기존매매평당가"]) / merged["기존매매평당가"] * 100.0
    )
    merged["분양가프리미엄_pct"] = merged["분양가프리미엄_pct"].round(1)
    merged["1차시장해석"] = merged.apply(classify_primary_market, axis=1)
    merged["심리해석"] = merged.apply(classify_sentiment, axis=1)
    merged["KB민간심리해석"] = merged.apply(classify_kb_sentiment, axis=1)
    merged["종합해석"] = merged.apply(classify_overall, axis=1)

    ordered_cols = [
        "권역",
        "시도",
        "기존매매평당가",
        "매매YoY",
        "거래회복률",
        "분양가격_기준월",
        "분양가격_최신",
        "분양가프리미엄_pct",
        "분양가격지수_최신",
        "분양가격지수_6개월변화",
        "초기분양률_기준월",
        "초기분양률_최신",
        "초기분양률_전년동기차",
        "분양세대수_최근6개월",
        "분양세대수_증감률pct",
        "대지비비율_기준월",
        "대지비비율_최신",
        "주택매매심리_기준월",
        "주택매매심리_최신",
        "주택매매심리_최근3개월",
        "주택매매심리_전년동기차",
        "부동산시장심리_최신",
        "KB_기준월",
        "KB_매수우위지수_최신",
        "KB_매수우위지수_최근3개월",
        "KB_매매가격전망지수_최신",
        "KB_매매가격전망지수_최근3개월",
        "KB_매매거래활발지수_최신",
        "KB_매매거래활발지수_최근3개월",
        "1차시장해석",
        "심리해석",
        "KB민간심리해석",
        "종합해석",
    ]
    merged = merged[ordered_cols].sort_values(["권역", "시도"])
    region = (
        merged.groupby("권역", as_index=False)
        .agg(
            기존매매평당가=("기존매매평당가", "mean"),
            매매YoY=("매매YoY", "mean"),
            거래회복률=("거래회복률", "mean"),
            분양가격_최신=("분양가격_최신", "mean"),
            분양가격지수_6개월변화=("분양가격지수_6개월변화", "mean"),
            초기분양률_최신=("초기분양률_최신", "mean"),
            초기분양률_전년동기차=("초기분양률_전년동기차", "mean"),
            분양세대수_최근6개월=("분양세대수_최근6개월", "sum"),
            분양세대수_증감률pct=("분양세대수_증감률pct", "mean"),
            주택매매심리_최근3개월=("주택매매심리_최근3개월", "mean"),
            주택매매심리_전년동기차=("주택매매심리_전년동기차", "mean"),
            부동산시장심리_최신=("부동산시장심리_최신", "mean"),
            KB_매수우위지수_최근3개월=("KB_매수우위지수_최근3개월", "mean"),
            KB_매매가격전망지수_최근3개월=("KB_매매가격전망지수_최근3개월", "mean"),
            KB_매매거래활발지수_최근3개월=("KB_매매거래활발지수_최근3개월", "mean"),
        )
        .round(2)
    )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    merged.to_csv(SIDO_OUT, index=False)
    region.to_csv(REGION_OUT, index=False)
    print(f"Wrote {SIDO_OUT}")
    print(f"Wrote {REGION_OUT}")


if __name__ == "__main__":
    main()
