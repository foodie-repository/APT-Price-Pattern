from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
DB_PATH = Path("/Volumes/T9/duckdb-analytics/db/apartment.duckdb")
OUT_DIR = ROOT / "04_결과" / "01_리포트_codex" / "00_공통"
NATIONAL_OUT = OUT_DIR / "00_금리대출여건_전국요약_20260315_codex.csv"
REGIONAL_OUT = OUT_DIR / "00_금리대출여건_시도요약_20260315_codex.csv"


def classify_financial_conditions(metrics: dict[str, float]) -> str:
    mortgage_rate = metrics.get("신규_주택담보대출금리")
    mortgage_yoy = metrics.get("주담대_예금취급기관_YoY")
    lending_attitude = metrics.get("대출태도_종합")
    loan_demand = metrics.get("대출수요_종합")

    if pd.notna(mortgage_rate) and pd.notna(mortgage_yoy) and mortgage_rate >= 4.2 and mortgage_yoy >= 5 and pd.notna(lending_attitude) and lending_attitude < 0:
        return "고금리·선별 레버리지 확장기"
    if pd.notna(mortgage_rate) and mortgage_rate < 3.5 and pd.notna(mortgage_yoy) and mortgage_yoy >= 6:
        return "저금리·레버리지 확장기"
    if pd.notna(loan_demand) and loan_demand < 0 and pd.notna(lending_attitude) and lending_attitude < -20:
        return "대출수요·공급 동반 위축기"
    return "중립·혼합"


def load_national_summary() -> pd.DataFrame:
    con = duckdb.connect(DB_PATH, read_only=True)
    rows: list[dict[str, object]] = []

    base_rate = con.execute(
        """
        WITH b AS (
            SELECT strptime(시점, '%Y.%m') AS dt, 기준금리
            FROM KOSIS_ECOS_기준금리
        ),
        l AS (SELECT MAX(dt) AS latest_dt FROM b)
        SELECT
            MAX(CASE WHEN dt = latest_dt THEN strftime(dt, '%Y-%m') END) AS 기준월,
            ROUND(MAX(CASE WHEN dt = latest_dt THEN 기준금리 END), 2) AS 최신값,
            ROUND(
                MAX(CASE WHEN dt = latest_dt THEN 기준금리 END)
                - MAX(CASE WHEN dt = latest_dt - INTERVAL 6 MONTH THEN 기준금리 END),
                2
            ) AS 변화_6개월,
            ROUND(
                MAX(CASE WHEN dt = latest_dt THEN 기준금리 END)
                - MAX(CASE WHEN dt = latest_dt - INTERVAL 12 MONTH THEN 기준금리 END),
                2
            ) AS 변화_12개월
        FROM b, l
        """
    ).df()
    base_rate_row = base_rate.iloc[0].to_dict()
    base_rate_row.update({"지표군": "금리", "지표명": "기준금리"})
    rows.append(base_rate_row)

    rate_configs = [
        ("KOSIS_BOK_대출금리_신규", "가계대출", "신규_가계대출금리"),
        ("KOSIS_BOK_대출금리_신규", "주택담보대출", "신규_주택담보대출금리"),
        ("KOSIS_BOK_대출금리_신규", "전세자금대출", "신규_전세자금대출금리"),
        ("KOSIS_BOK_대출금리_신규", "고정형 주택담보대출 3)", "신규_고정형주담대금리"),
        ("KOSIS_BOK_대출금리_신규", "변동형 주택담보대출 3)", "신규_변동형주담대금리"),
        ("KOSIS_BOK_대출금리_잔액", "가계대출 2)", "잔액_가계대출금리"),
        ("KOSIS_BOK_대출금리_잔액", "주택담보대출", "잔액_주택담보대출금리"),
        ("KOSIS_BOK_대출금리_잔액", "전세자금대출", "잔액_전세자금대출금리"),
    ]
    for table_name, loan_type, metric_name in rate_configs:
        df = con.execute(
            f"""
            WITH b AS (
                SELECT strptime(시점, '%Y.%m') AS dt, 금리
                FROM {table_name}
                WHERE 대출유형 = ?
            ),
            l AS (SELECT MAX(dt) AS latest_dt FROM b)
            SELECT
                MAX(CASE WHEN dt = latest_dt THEN strftime(dt, '%Y-%m') END) AS 기준월,
                ROUND(MAX(CASE WHEN dt = latest_dt THEN 금리 END), 2) AS 최신값,
                ROUND(
                    MAX(CASE WHEN dt = latest_dt THEN 금리 END)
                    - MAX(CASE WHEN dt = latest_dt - INTERVAL 6 MONTH THEN 금리 END),
                    2
                ) AS 변화_6개월,
                ROUND(
                    MAX(CASE WHEN dt = latest_dt THEN 금리 END)
                    - MAX(CASE WHEN dt = latest_dt - INTERVAL 12 MONTH THEN 금리 END),
                    2
                ) AS 변화_12개월
            FROM b, l
            """
            ,
            [loan_type],
        ).df()
        row = df.iloc[0].to_dict()
        row.update({"지표군": "금리", "지표명": metric_name})
        rows.append(row)

    loan_configs = [
        ("KOSIS_BOK_가계대출_업권별", "업권", "예금취급기관", "가계대출_예금취급기관"),
        ("KOSIS_BOK_가계대출_업권별", "업권", "예금은행", "가계대출_예금은행"),
        ("KOSIS_BOK_가계대출_업권별", "업권", "비은행예금취급기관", "가계대출_비은행"),
        ("KOSIS_BOK_가계대출_용도별", "용도", "주택담보대출-예금취급기관", "주담대_예금취급기관"),
        ("KOSIS_BOK_가계대출_용도별", "용도", "주택담보대출-예금은행", "주담대_예금은행"),
        ("KOSIS_BOK_가계대출_용도별", "용도", "기타대출-예금취급기관", "기타대출_예금취급기관"),
        ("KOSIS_BOK_가계대출_용도별", "용도", "[참고] 주택금융공사 및 주택도시기금의 정책대출", "정책대출"),
    ]
    for table_name, column_name, value_name, metric_name in loan_configs:
        df = con.execute(
            f"""
            WITH b AS (
                SELECT strptime(시점, '%Y.%m') AS dt, 잔액_십억원
                FROM {table_name}
                WHERE {column_name} = ?
            ),
            l AS (SELECT MAX(dt) AS latest_dt FROM b)
            SELECT
                MAX(CASE WHEN dt = latest_dt THEN strftime(dt, '%Y-%m') END) AS 기준월,
                ROUND(MAX(CASE WHEN dt = latest_dt THEN 잔액_십억원 END), 1) AS 최신값,
                ROUND(
                    100.0 * (
                        MAX(CASE WHEN dt = latest_dt THEN 잔액_십억원 END)
                        / NULLIF(MAX(CASE WHEN dt = latest_dt - INTERVAL 6 MONTH THEN 잔액_십억원 END), 0) - 1
                    ),
                    1
                ) AS 변화_6개월,
                ROUND(
                    100.0 * (
                        MAX(CASE WHEN dt = latest_dt THEN 잔액_십억원 END)
                        / NULLIF(MAX(CASE WHEN dt = latest_dt - INTERVAL 12 MONTH THEN 잔액_십억원 END), 0) - 1
                    ),
                    1
                ) AS 변화_12개월
            FROM b, l
            """
            ,
            [value_name],
        ).df()
        row = df.iloc[0].to_dict()
        row.update({"지표군": "가계대출", "지표명": metric_name})
        rows.append(row)

    survey_configs = [
        ("KOSIS_BOK_대출수요", "대출수요_종합"),
        ("KOSIS_BOK_대출태도", "대출태도_종합"),
    ]
    for table_name, metric_name in survey_configs:
        df = con.execute(
            f"""
            WITH b AS (
                SELECT try_strptime(substr(시점, 1, 7), '%Y.%m') AS dt, 시점, 지수
                FROM {table_name}
                WHERE 항목 = '국내은행-차주가중종합지수'
            ),
            l AS (SELECT MAX(dt) AS latest_dt FROM b)
            SELECT
                MAX(CASE WHEN dt = latest_dt THEN 시점 END) AS 기준월,
                ROUND(MAX(CASE WHEN dt = latest_dt THEN 지수 END), 1) AS 최신값,
                NULL AS 변화_6개월,
                ROUND(
                    MAX(CASE WHEN dt = latest_dt THEN 지수 END)
                    - MAX(CASE WHEN dt = latest_dt - INTERVAL 12 MONTH THEN 지수 END),
                    1
                ) AS 변화_12개월
            FROM b, l
            """
        ).df()
        row = df.iloc[0].to_dict()
        row.update({"지표군": "대출행태", "지표명": metric_name})
        rows.append(row)

    pir_df = con.execute(
        """
        WITH b AS (
            SELECT 날짜, 지역명, KB아파트PIR
            FROM KB_아파트담보대출PIR
            WHERE 지역명 IN ('서울', '경기', '인천')
        ),
        l AS (SELECT MAX(날짜) AS latest_dt FROM b)
        SELECT
            MAX(CASE WHEN 날짜 = latest_dt THEN strftime(날짜, '%Y-%m') END) AS 기준월,
            지역명,
            ROUND(MAX(CASE WHEN 날짜 = latest_dt THEN KB아파트PIR END), 2) AS 최신값,
            NULL AS 변화_6개월,
            ROUND(
                MAX(CASE WHEN 날짜 = latest_dt THEN KB아파트PIR END)
                - MAX(CASE WHEN 날짜 = latest_dt - INTERVAL 12 MONTH THEN KB아파트PIR END),
                2
            ) AS 변화_12개월
        FROM b, l
        GROUP BY 지역명, latest_dt
        ORDER BY 지역명
        """
    ).df()
    for _, rec in pir_df.iterrows():
        rows.append(
            {
                "지표군": "부담지표",
                "지표명": f"KB_PIR_{rec['지역명']}",
                "기준월": rec["기준월"],
                "최신값": rec["최신값"],
                "변화_6개월": rec["변화_6개월"],
                "변화_12개월": rec["변화_12개월"],
            }
        )

    out = pd.DataFrame(rows)
    metrics = dict(zip(out["지표명"], out["최신값"]))
    yoy_map = dict(zip(out["지표명"], out["변화_12개월"]))
    metrics["주담대_예금취급기관_YoY"] = yoy_map.get("주담대_예금취급기관")
    out["금융여건해석"] = classify_financial_conditions(metrics)
    return out


def load_regional_summary() -> pd.DataFrame:
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute(
        """
        WITH b AS (
            SELECT strptime(시점, '%Y.%m') AS dt, 대출유형, 시도, 잔액_십억원
            FROM KOSIS_BOK_가계대출_지역별
            WHERE 대출유형 IN ('예금은행', '주택담보대출-예금은행')
        ),
        l AS (SELECT MAX(dt) AS latest_dt FROM b)
        SELECT
            MAX(CASE WHEN dt = latest_dt THEN strftime(dt, '%Y-%m') END) AS 기준월,
            시도,
            ROUND(MAX(CASE WHEN 대출유형 = '예금은행' AND dt = latest_dt THEN 잔액_십억원 END), 1) AS 예금은행가계대출_최신,
            ROUND(
                100.0 * (
                    MAX(CASE WHEN 대출유형 = '예금은행' AND dt = latest_dt THEN 잔액_십억원 END)
                    / NULLIF(MAX(CASE WHEN 대출유형 = '예금은행' AND dt = latest_dt - INTERVAL 12 MONTH THEN 잔액_십억원 END), 0) - 1
                ),
                1
            ) AS 예금은행가계대출_YoY,
            ROUND(
                100.0 * (
                    MAX(CASE WHEN 대출유형 = '예금은행' AND dt = latest_dt THEN 잔액_십억원 END)
                    / NULLIF(MAX(CASE WHEN 대출유형 = '예금은행' AND dt = latest_dt - INTERVAL 6 MONTH THEN 잔액_십억원 END), 0) - 1
                ),
                1
            ) AS 예금은행가계대출_6개월,
            ROUND(MAX(CASE WHEN 대출유형 = '주택담보대출-예금은행' AND dt = latest_dt THEN 잔액_십억원 END), 1) AS 예금은행주담대_최신,
            ROUND(
                100.0 * (
                    MAX(CASE WHEN 대출유형 = '주택담보대출-예금은행' AND dt = latest_dt THEN 잔액_십억원 END)
                    / NULLIF(MAX(CASE WHEN 대출유형 = '주택담보대출-예금은행' AND dt = latest_dt - INTERVAL 12 MONTH THEN 잔액_십억원 END), 0) - 1
                ),
                1
            ) AS 예금은행주담대_YoY,
            ROUND(
                100.0 * (
                    MAX(CASE WHEN 대출유형 = '주택담보대출-예금은행' AND dt = latest_dt THEN 잔액_십억원 END)
                    / NULLIF(MAX(CASE WHEN 대출유형 = '주택담보대출-예금은행' AND dt = latest_dt - INTERVAL 6 MONTH THEN 잔액_십억원 END), 0) - 1
                ),
                1
            ) AS 예금은행주담대_6개월
        FROM b, l
        GROUP BY 시도, latest_dt
        ORDER BY 시도
        """
    ).df()
    df["주담대비중_pct"] = (df["예금은행주담대_최신"] / df["예금은행가계대출_최신"] * 100.0).round(1)
    df["지역해석"] = pd.Series(index=df.index, dtype="object")
    df.loc[df["예금은행주담대_YoY"] >= 9, "지역해석"] = "주담대 확장 상위"
    df.loc[df["지역해석"].isna() & (df["예금은행주담대_YoY"] <= 4), "지역해석"] = "주담대 증가 둔화"
    df.loc[df["지역해석"].isna(), "지역해석"] = "중립·완만"
    return df.sort_values(["예금은행주담대_YoY", "예금은행가계대출_YoY"], ascending=[False, False])


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    national = load_national_summary()
    regional = load_regional_summary()
    national.to_csv(NATIONAL_OUT, index=False)
    regional.to_csv(REGIONAL_OUT, index=False)
    print(f"Wrote {NATIONAL_OUT}")
    print(f"Wrote {REGIONAL_OUT}")


if __name__ == "__main__":
    main()
