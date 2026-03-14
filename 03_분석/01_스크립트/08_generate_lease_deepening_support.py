from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
DB_PATH = Path("/Volumes/T9/duckdb-analytics/db/apartment.duckdb")
OUT_DIR = ROOT / "04_결과/01_리포트_codex"
SALE_DIR = OUT_DIR / "01_매매시장"
LEASE_DIR = OUT_DIR / "02_임차시장"
PRED_DIR = OUT_DIR / "06_예측검증"
LABEL_PATH = SALE_DIR / "01_매매시장_행동라벨_20260314_codex.csv"
PRED_PATH = PRED_DIR / "06_예측점수_20260313_codex_시군구.csv"

SIMILAR_PATH = LEASE_DIR / "02_임차시장_유사국면비교_20260314_codex.csv"
VALIDATION_PATH = LEASE_DIR / "02_임차시장_행동라벨임차검증_20260314_codex.csv"
TYPE_PATH = LEASE_DIR / "02_임차시장_유형분류_20260314_codex.csv"


def shift_ym(ym: int, delta: int) -> int:
    year, month = divmod(int(ym), 100)
    month += delta
    while month <= 0:
        year -= 1
        month += 12
    while month > 12:
        year += 1
        month -= 12
    return year * 100 + month


def month_diff(a: float | int | None, b: float | int | None) -> float:
    if pd.isna(a) or pd.isna(b):
        return np.nan
    ay, am = divmod(int(a), 100)
    by, bm = divmod(int(b), 100)
    return (ay - by) * 12 + (am - bm)


def region_case(alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return f"""
    CASE
      WHEN {prefix}시도 IN ('서울특별시', '경기도', '인천광역시') THEN '수도권'
      WHEN {prefix}시도 IN ('부산광역시', '대구광역시', '광주광역시', '대전광역시', '울산광역시') THEN '지방광역시'
      ELSE '기타지방'
    END
    """


def build_similarity_frame(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    q_ratio = f"""
    SELECT
        계약년월,
        {region_case()} AS 권역,
        MEDIAN(전세가율_pct) AS jeonse_ratio_med
    FROM v_jeonse_ratio_monthly
    WHERE 전용면적_구분 = '중소형'
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    q_mix = f"""
    SELECT
        계약년월,
        {region_case()} AS 권역,
        100.0 * SUM(월세거래건수) / NULLIF(SUM(전체거래건수), 0) AS wolse_share_pct
    FROM v_lease_conversion_mix_monthly
    WHERE 전용면적_구분 = '중소형'
    GROUP BY 1, 2
    ORDER BY 1, 2
    """

    ratio = con.execute(q_ratio).df()
    mix = con.execute(q_mix).df()
    ratio["jeonse_ratio_change_12m_pp"] = ratio.groupby("권역")["jeonse_ratio_med"].diff(12)

    base = ratio.merge(mix, on=["계약년월", "권역"], how="inner")
    wide = base.pivot(
        index="계약년월",
        columns="권역",
        values=["jeonse_ratio_med", "jeonse_ratio_change_12m_pp", "wolse_share_pct"],
    )
    wide.columns = [f"{metric}_{region}" for metric, region in wide.columns]
    wide = wide.sort_index()

    rolling = wide.rolling(3).mean().dropna().copy()
    current_end = int(rolling.index.max())

    zscore = (rolling - rolling.mean()) / rolling.std(ddof=0)
    current = zscore.loc[current_end]
    distances = ((zscore[zscore.index < current_end] - current) ** 2).sum(axis=1).pow(0.5).sort_values()

    top_candidates: list[tuple[int, float]] = []
    for ym, distance in distances.items():
        if not top_candidates or all(abs(month_diff(ym, picked_ym)) >= 4 for picked_ym, _ in top_candidates):
            top_candidates.append((int(ym), float(distance)))
        if len(top_candidates) >= 8:
            break

    similar_1 = top_candidates[0]
    similar_2 = top_candidates[1]
    reference = next(((ym, dist) for ym, dist in top_candidates if ym <= 201912), top_candidates[2])

    rows = [
        ("현재국면", current_end, 0.0),
        ("유사국면1", similar_1[0], similar_1[1]),
        ("유사국면2", similar_2[0], similar_2[1]),
        ("장기참고국면", reference[0], reference[1]),
    ]

    records: list[dict[str, float | int | str]] = []
    for label, end_ym, distance in rows:
        row = rolling.loc[end_ym]
        records.append(
            {
                "구간": label,
                "시작년월": shift_ym(end_ym, -2),
                "종료년월": end_ym,
                "거리": round(distance, 3),
                "수도권_전세가율": round(row["jeonse_ratio_med_수도권"], 2),
                "지방광역시_전세가율": round(row["jeonse_ratio_med_지방광역시"], 2),
                "기타지방_전세가율": round(row["jeonse_ratio_med_기타지방"], 2),
                "수도권_전세가율12개월변화_pp": round(row["jeonse_ratio_change_12m_pp_수도권"], 2),
                "지방광역시_전세가율12개월변화_pp": round(row["jeonse_ratio_change_12m_pp_지방광역시"], 2),
                "기타지방_전세가율12개월변화_pp": round(row["jeonse_ratio_change_12m_pp_기타지방"], 2),
                "수도권_월세비중": round(row["wolse_share_pct_수도권"], 2),
                "지방광역시_월세비중": round(row["wolse_share_pct_지방광역시"], 2),
                "기타지방_월세비중": round(row["wolse_share_pct_기타지방"], 2),
            }
        )
    return pd.DataFrame(records)


def build_validation_frame(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    labels = pd.read_csv(LABEL_PATH)
    pred = pd.read_csv(PRED_PATH)
    pred_current = pred[
        [
            "시도",
            "시군구",
            "price_12m_change_pct",
            "jeonse_ratio_pct",
            "wolse_share_pct",
            "trade_recovery_pct",
        ]
    ].copy()

    regions = labels[["시도", "시군구"]].drop_duplicates()
    region_sql = " UNION ALL ".join(
        [f"SELECT '{row.시도}' AS 시도, '{row.시군구}' AS 시군구" for row in regions.itertuples(index=False)]
    )

    q_ratio_mix = f"""
    WITH target AS (
        {region_sql}
    ),
    ratio AS (
        SELECT
            시도,
            시군구,
            MEDIAN(전세가율_pct) FILTER (WHERE 계약년월 IN (202601, 202602, 202603)) AS recent_jeonse_ratio_3m
        FROM v_jeonse_ratio_monthly
        WHERE 전용면적_구분 = '중소형'
        GROUP BY 1, 2
    ),
    mix AS (
        SELECT
            시도,
            시군구,
            100.0 * SUM(월세거래건수) FILTER (WHERE 계약년월 IN (202601, 202602, 202603))
                / NULLIF(SUM(전체거래건수) FILTER (WHERE 계약년월 IN (202601, 202602, 202603)), 0) AS recent_wolse_share_3m,
            100.0 * SUM(전세거래건수) FILTER (WHERE 계약년월 IN (202601, 202602, 202603))
                / NULLIF(SUM(전체거래건수) FILTER (WHERE 계약년월 IN (202601, 202602, 202603)), 0) AS recent_jeonse_share_3m
        FROM v_lease_conversion_mix_monthly
        WHERE 전용면적_구분 = '중소형'
        GROUP BY 1, 2
    ),
    turn_sale AS (
        SELECT
            시도,
            시군구,
            MIN(계약년월) AS sale_turn_ym
        FROM (
            SELECT
                시도,
                시군구,
                계약년월,
                MEDIAN(매매대표평당가_YoY_pct) AS yoy
            FROM v_sale_monthly_yoy
            WHERE 전용면적_구분 = '중소형'
              AND 계약년월 >= 202301
            GROUP BY 1, 2, 3
        )
        WHERE yoy > 0
        GROUP BY 1, 2
    ),
    turn_jeonse AS (
        SELECT
            시도,
            시군구,
            MIN(계약년월) AS jeonse_turn_ym
        FROM (
            SELECT
                시도,
                시군구,
                계약년월,
                MEDIAN(전세대표평당가_YoY_pct) AS yoy
            FROM v_jeonse_monthly_yoy
            WHERE 전용면적_구분 = '중소형'
              AND 계약년월 >= 202301
            GROUP BY 1, 2, 3
        )
        WHERE yoy > 0
        GROUP BY 1, 2
    )
    SELECT
        t.시도,
        t.시군구,
        r.recent_jeonse_ratio_3m,
        m.recent_jeonse_share_3m,
        m.recent_wolse_share_3m,
        ts.sale_turn_ym,
        tj.jeonse_turn_ym
    FROM target t
    LEFT JOIN ratio r USING (시도, 시군구)
    LEFT JOIN mix m USING (시도, 시군구)
    LEFT JOIN turn_sale ts USING (시도, 시군구)
    LEFT JOIN turn_jeonse tj USING (시도, 시군구)
    ORDER BY 1, 2
    """
    ratio_mix = con.execute(q_ratio_mix).df()
    ratio_mix["lead_lag_months"] = [
        month_diff(j, s) for j, s in zip(ratio_mix["jeonse_turn_ym"], ratio_mix["sale_turn_ym"])
    ]

    merged = labels.merge(pred_current, on=["시도", "시군구"], how="left", suffixes=("", "_pred"))
    merged = merged.merge(ratio_mix, on=["시도", "시군구"], how="left")

    merged["임차수요기반양호"] = np.where(
        (merged["recent_jeonse_ratio_3m"] >= 65) & (merged["recent_wolse_share_3m"] < 30),
        "예",
        "아니오",
    )
    merged["월세비중높음"] = np.where(merged["recent_wolse_share_3m"] >= 40, "예", "아니오")
    merged["임차선행"] = np.where(
        (merged["lead_lag_months"] <= 0) & (merged["recent_jeonse_ratio_3m"] >= 60),
        "예",
        "아니오",
    )
    merged["기다림필요"] = np.where(
        merged["심화행동라벨"].isin(["관찰 유지", "보수 접근"]) & (merged["임차수요기반양호"] == "아니오"),
        "예",
        "아니오",
    )

    def build_reason(row: pd.Series) -> str:
        reasons: list[str] = []
        if pd.notna(row.get("recent_jeonse_ratio_3m")):
            reasons.append(f"전세가율 {row['recent_jeonse_ratio_3m']:.1f}%")
        if pd.notna(row.get("recent_wolse_share_3m")):
            reasons.append(f"월세비중 {row['recent_wolse_share_3m']:.1f}%")
        if pd.notna(row.get("lead_lag_months")):
            if row["lead_lag_months"] < 0:
                reasons.append(f"전세가 매매보다 {abs(int(row['lead_lag_months']))}개월 선행")
            elif row["lead_lag_months"] > 0:
                reasons.append(f"매매가 전세보다 {int(row['lead_lag_months'])}개월 선행")
            else:
                reasons.append("전세와 매매 전환 시점이 거의 동시")
        if row["월세비중높음"] == "예":
            reasons.append("월세 비중 높음")
        if row["임차수요기반양호"] == "예":
            reasons.append("임차 수요 기반 양호")
        if row["임차선행"] == "예":
            reasons.append("임차 선행")
        return ", ".join(reasons)

    merged["임차검증메모"] = merged.apply(build_reason, axis=1)
    columns = [
        "심화행동라벨",
        "시도",
        "시군구",
        "예측분류",
        "투자검토분류",
        "상승확률점수",
        "투자적합성점수",
        "price_12m_change_pct",
        "trade_recovery_pct",
        "recent_jeonse_ratio_3m",
        "recent_jeonse_share_3m",
        "recent_wolse_share_3m",
        "sale_turn_ym",
        "jeonse_turn_ym",
        "lead_lag_months",
        "임차수요기반양호",
        "월세비중높음",
        "임차선행",
        "기다림필요",
        "임차검증메모",
    ]
    return merged[columns].sort_values(["심화행동라벨", "상승확률점수"], ascending=[True, False]).reset_index(drop=True)


def build_type_frame(validation: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    for row in validation.itertuples(index=False):
        if row.기다림필요 == "예":
            records.append(
                {
                    "구분": "기다려야 할 지역",
                    "시도": row.시도,
                    "시군구": row.시군구,
                    "심화행동라벨": row.심화행동라벨,
                    "근거": row.임차검증메모,
                }
            )
        if row.월세비중높음 == "예":
            records.append(
                {
                    "구분": "월세 비중 높아 해석 주의 지역",
                    "시도": row.시도,
                    "시군구": row.시군구,
                    "심화행동라벨": row.심화행동라벨,
                    "근거": row.임차검증메모,
                }
            )
        if row.임차선행 == "예":
            records.append(
                {
                    "구분": "매매보다 임차가 먼저 강한 지역",
                    "시도": row.시도,
                    "시군구": row.시군구,
                    "심화행동라벨": row.심화행동라벨,
                    "근거": row.임차검증메모,
                }
            )
    return pd.DataFrame(records).drop_duplicates().reset_index(drop=True)


def main() -> None:
    LEASE_DIR.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        similar = build_similarity_frame(con)
        validation = build_validation_frame(con)
    type_frame = build_type_frame(validation)

    similar.to_csv(SIMILAR_PATH, index=False, encoding="utf-8-sig")
    validation.to_csv(VALIDATION_PATH, index=False, encoding="utf-8-sig")
    type_frame.to_csv(TYPE_PATH, index=False, encoding="utf-8-sig")

    print(f"Wrote {SIMILAR_PATH.relative_to(ROOT)}")
    print(f"Wrote {VALIDATION_PATH.relative_to(ROOT)}")
    print(f"Wrote {TYPE_PATH.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
