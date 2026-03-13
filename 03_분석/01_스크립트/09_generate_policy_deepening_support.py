from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
DB_PATH = Path("/Volumes/T9/duckdb-analytics/db/apartment.duckdb")
OUT_DIR = ROOT / "04_결과" / "01_리포트_codex"
GRADE_PATH = ROOT / "02_데이터" / "02_참조" / "수도권_매매_급지표_시군구_20260311.csv"
TOHUGA_REF_PATH = ROOT / "02_데이터" / "02_참조" / "토지거래허가구역_활성참조_20260313.csv"
SALE_ACTION_PATH = OUT_DIR / "01_매매시장_행동라벨_20260314_codex.csv"
LEASE_VALIDATION_PATH = OUT_DIR / "02_임차시장_행동라벨임차검증_20260314_codex.csv"
INVESTMENT_PATH = OUT_DIR / "05_투자검토대상군_20260313_codex_시군구.csv"

WINDOW_OUT = OUT_DIR / "03_정책국면_단기중기비교_20260314_codex.csv"
CONTROL_OUT = OUT_DIR / "03_정책_2순위대조군_20260314_codex.csv"
TOHUGA_OUT = OUT_DIR / "03_토허현재해석_20260314_codex.csv"
RECOVERY_OUT = OUT_DIR / "03_정책후회복후보_20260314_codex.csv"


def add_months(ym: int, n: int) -> int:
    year = ym // 100
    month = ym % 100
    month_index = month - 1 + n
    year += month_index // 12
    month = month_index % 12 + 1
    return year * 100 + month


def region_tuples(df: pd.DataFrame) -> set[tuple[str, str]]:
    return set(map(tuple, df[["시도", "시군구"]].drop_duplicates().to_records(index=False).tolist()))


def build_monthly_metrics(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    sale = con.execute(
        """
        SELECT
            시도,
            시군구,
            계약년월,
            median(거래금액_만원 / NULLIF(전용면적_㎡ / 3.305785, 0)) AS sale_pp,
            count(*) AS sale_trades
        FROM v_sale_clean
        WHERE 주택유형 = '아파트'
          AND 계약년월 BETWEEN 202201 AND 202602
        GROUP BY 1, 2, 3
        """
    ).df()
    jeonse = con.execute(
        """
        SELECT
            시도,
            시군구,
            계약년월,
            median(보증금_만원 / NULLIF(전용면적_㎡ / 3.305785, 0)) AS jeonse_pp,
            count(*) AS jeonse_trades
        FROM v_jeonse_clean
        WHERE 주택유형 = '아파트'
          AND 계약년월 BETWEEN 202201 AND 202602
        GROUP BY 1, 2, 3
        """
    ).df()
    lease_mix = con.execute(
        """
        WITH jeonse AS (
            SELECT 시도, 시군구, 계약년월, count(*) AS jeonse_count
            FROM v_jeonse_clean
            WHERE 주택유형 = '아파트'
              AND 계약년월 BETWEEN 202201 AND 202602
            GROUP BY 1, 2, 3
        ),
        wolse AS (
            SELECT 시도, 시군구, 계약년월, count(*) AS wolse_count
            FROM v_wolse_clean
            WHERE 주택유형 = '아파트'
              AND 계약년월 BETWEEN 202201 AND 202602
            GROUP BY 1, 2, 3
        )
        SELECT
            coalesce(jeonse.시도, wolse.시도) AS 시도,
            coalesce(jeonse.시군구, wolse.시군구) AS 시군구,
            coalesce(jeonse.계약년월, wolse.계약년월) AS 계약년월,
            coalesce(jeonse_count, 0) AS jeonse_count,
            coalesce(wolse_count, 0) AS wolse_count,
            100.0 * coalesce(wolse_count, 0)
                / NULLIF(coalesce(jeonse_count, 0) + coalesce(wolse_count, 0), 0) AS wolse_share
        FROM jeonse
        FULL OUTER JOIN wolse USING (시도, 시군구, 계약년월)
        """
    ).df()
    return sale.merge(jeonse, on=["시도", "시군구", "계약년월"], how="outer").merge(
        lease_mix, on=["시도", "시군구", "계약년월"], how="outer"
    )


def build_pre_metrics(monthly: pd.DataFrame, grade: pd.DataFrame, event_ym: int) -> pd.DataFrame:
    start_ym = add_months(event_ym, -6)
    end_ym = add_months(event_ym, -1)
    pre = monthly[(monthly["계약년월"] >= start_ym) & (monthly["계약년월"] <= end_ym)].copy()
    out = (
        pre.groupby(["시도", "시군구"], as_index=False)
        .agg(pre6_sale_pp=("sale_pp", "mean"), pre6_sale_trades=("sale_trades", "mean"))
    )
    anchor = monthly.loc[monthly["계약년월"] == end_ym, ["시도", "시군구", "sale_pp"]].rename(
        columns={"sale_pp": "anchor_sale_pp"}
    )
    prev = monthly.loc[
        monthly["계약년월"] == add_months(end_ym, -12), ["시도", "시군구", "sale_pp"]
    ].rename(columns={"sale_pp": "prev12_sale_pp"})
    out = out.merge(anchor, on=["시도", "시군구"], how="left").merge(
        prev, on=["시도", "시군구"], how="left"
    )
    out["pre12_change_pct"] = (out["anchor_sale_pp"] / out["prev12_sale_pp"] - 1) * 100.0
    return out.merge(grade[["시도", "시군구", "급지_점수", "급지그룹"]], on=["시도", "시군구"], how="left")


def match_secondary_controls(
    monthly: pd.DataFrame,
    grade: pd.DataFrame,
    event_ym: int,
    treatment: set[tuple[str, str]],
    universe_filter,
    label: str,
    top_n: int,
) -> tuple[pd.DataFrame, set[tuple[str, str]]]:
    pre = build_pre_metrics(monthly, grade, event_ym)
    treatment_frame = pre[pre[["시도", "시군구"]].apply(tuple, axis=1).isin(treatment)].copy()
    candidate_frame = pre[universe_filter(pre)].copy()
    features = ["pre6_sale_pp", "pre6_sale_trades", "pre12_change_pct", "급지_점수"]
    target_mean = treatment_frame[features].mean()
    scale = candidate_frame[features].std().replace(0, 1).fillna(1)
    candidate_frame["distance"] = np.sqrt((((candidate_frame[features] - target_mean) / scale) ** 2).sum(axis=1))
    candidate_frame["정책국면"] = label
    candidate_frame["selected"] = 0
    candidate_frame = candidate_frame.sort_values("distance").reset_index(drop=True)
    candidate_frame.loc[: top_n - 1, "selected"] = 1
    selected = region_tuples(candidate_frame.loc[candidate_frame["selected"] == 1, ["시도", "시군구"]])
    return candidate_frame, selected


def summarize_group(
    monthly: pd.DataFrame,
    policy_name: str,
    analysis_frame: str,
    group_name: str,
    event_ym: int,
    regions: set[tuple[str, str]],
) -> dict[str, object]:
    frame = monthly[monthly[["시도", "시군구"]].apply(tuple, axis=1).isin(regions)].copy()
    windows = {
        "pre6": (add_months(event_ym, -6), add_months(event_ym, -1)),
        "short0_3": (event_ym, add_months(event_ym, 2)),
        "mid3_12": (add_months(event_ym, 3), min(202602, add_months(event_ym, 11))),
    }
    row: dict[str, object] = {
        "정책국면": policy_name,
        "분석프레임": analysis_frame,
        "비교그룹": group_name,
        "이벤트월": event_ym,
        "지역수": len(regions),
        "지역목록": "; ".join(f"{sido} {sigungu}" for sido, sigungu in sorted(regions)),
    }
    for key, (start_ym, end_ym) in windows.items():
        sub = frame[(frame["계약년월"] >= start_ym) & (frame["계약년월"] <= end_ym)]
        row[f"{key}_관측개월수"] = int(sub["계약년월"].nunique())
        row[f"{key}_매매평당가"] = sub["sale_pp"].mean()
        row[f"{key}_전세평당가"] = sub["jeonse_pp"].mean()
        row[f"{key}_매매거래량"] = sub["sale_trades"].mean()
        row[f"{key}_월세비중"] = sub["wolse_share"].mean()
    for metric in ["매매평당가", "전세평당가", "매매거래량", "월세비중"]:
        row[f"short0_3_{metric}_변화율_pct"] = (
            (row[f"short0_3_{metric}"] / row[f"pre6_{metric}"] - 1) * 100.0
            if pd.notna(row[f"pre6_{metric}"]) and row[f"pre6_{metric}"] not in {0, 0.0}
            else np.nan
        )
        row[f"mid3_12_{metric}_변화율_pct"] = (
            (row[f"mid3_12_{metric}"] / row[f"pre6_{metric}"] - 1) * 100.0
            if pd.notna(row[f"pre6_{metric}"]) and row[f"pre6_{metric}"] not in {0, 0.0}
            else np.nan
        )
    return row


def build_stock_unsold_frame(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    latest_unsold = con.execute(
        """
        SELECT DISTINCT 시도, 시군구 AS broad
        FROM KOSIS_준공후미분양
        WHERE 시점 = (SELECT max(시점) FROM KOSIS_준공후미분양)
        """
    ).df()
    city_roots: dict[str, list[tuple[str, str]]] = {}
    for sido, group in latest_unsold.groupby("시도"):
        city_roots[sido] = []
        for broad in group["broad"].tolist():
            root = broad[:-1] if broad.endswith("시") else broad
            city_roots[sido].append((root.replace(" ", ""), broad))

    product = con.execute(
        """
        SELECT 시도, 시군구, 세대수
        FROM v_complex_product_national
        WHERE 세대수 IS NOT NULL
        """
    ).df()

    def normalize_broad(row: pd.Series) -> str:
        if row["시도"] == "서울특별시":
            return row["시군구"]
        raw = str(row["시군구"]).replace(" ", "")
        for root, broad in city_roots.get(row["시도"], []):
            if raw == broad.replace(" ", "") or raw.startswith(root):
                return broad
        return row["시군구"]

    product["broad"] = product.apply(normalize_broad, axis=1)
    stock = (
        product.groupby(["시도", "broad"], as_index=False)["세대수"]
        .sum()
        .rename(columns={"세대수": "stock_units"})
    )
    unsold = con.execute(
        """
        SELECT 시도, 시군구 AS broad, 미분양수 AS completed_unsold
        FROM KOSIS_준공후미분양
        WHERE 시점 = (SELECT max(시점) FROM KOSIS_준공후미분양)
        """
    ).df()
    out = stock.merge(unsold, on=["시도", "broad"], how="left")
    out["completed_unsold_ratio_pct"] = out["completed_unsold"] / out["stock_units"] * 100.0
    return out


def build_tohuga_context(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    ref = pd.read_csv(TOHUGA_REF_PATH, encoding="utf-8-sig")
    rows: list[dict[str, object]] = []
    for item in ref.to_dict("records"):
        sigungu_value = item["시군구"]
        if isinstance(sigungu_value, str) and ";" in sigungu_value:
            sigungus = [part.strip() for part in sigungu_value.split(";") if part.strip()]
        else:
            sigungus = [sigungu_value]
        for sigungu in sigungus:
            row = dict(item)
            row["시군구"] = sigungu
            rows.append(row)
    exploded = pd.DataFrame(rows)

    latest = con.execute("SELECT * FROM v_tohuga_latest_collection_sigungu").df()
    recent = con.execute("SELECT * FROM v_tohuga_recent_sigungu_summary").df()
    join_cols = ["시도", "시군구"]
    latest = latest[join_cols + ["최신수집일자", "최신수집허가건수"]]
    recent = recent[
        join_cols + ["최근30일허가건수", "최근60일허가건수", "토허지연해석주의", "토허지연해석메모"]
    ]
    out = exploded.merge(latest, on=join_cols, how="left").merge(recent, on=join_cols, how="left")
    out["토허현재해석"] = np.where(
        out["최근30일허가건수"].fillna(0) > 0,
        "최근 30일 허가 승인 존재. 최근 1~2개월 실거래 약세는 지연 반영 가능성과 함께 해석",
        "허가 승인 최근 흐름 없음 또는 시군구 직접 매칭 어려움",
    )
    columns = [
        "확인기준일",
        "시도",
        "시군구",
        "세부구역명",
        "적용범위요약",
        "현재판정",
        "근거요약",
        "출처기준일",
        "출처유형",
        "출처URL",
        "지정시작일",
        "지정종료일",
        "최신수집일자",
        "최신수집허가건수",
        "최근30일허가건수",
        "최근60일허가건수",
        "토허지연해석주의",
        "토허지연해석메모",
        "토허현재해석",
        "비고",
    ]
    return out[columns].sort_values(["시도", "시군구", "세부구역명"]).reset_index(drop=True)


def build_recovery_candidates(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    sale_action = pd.read_csv(SALE_ACTION_PATH, encoding="utf-8-sig")
    lease_validation = pd.read_csv(LEASE_VALIDATION_PATH, encoding="utf-8-sig")
    investment = pd.read_csv(INVESTMENT_PATH, encoding="utf-8-sig")
    stock_unsold = build_stock_unsold_frame(con)

    investment["broad"] = investment["시군구_상위"].fillna(investment["시군구"])
    merged = sale_action.merge(
        lease_validation[
            ["시도", "시군구", "임차수요기반양호", "월세화경계", "임차선행", "기다림필요", "임차검증메모"]
        ],
        on=["시도", "시군구"],
        how="left",
    ).merge(
        investment[
            [
                "시도",
                "시군구",
                "broad",
                "정책메모",
                "투자적합성점수",
                "상승확률점수",
                "price_12m_change_pct",
                "trade_recovery_pct",
                "jeonse_ratio_pct",
                "wolse_share_pct",
                "투자검토분류",
            ]
        ],
        on=["시도", "시군구"],
        how="left",
        suffixes=("_sale", "_cand"),
    ).merge(stock_unsold[["시도", "broad", "completed_unsold", "completed_unsold_ratio_pct"]], on=["시도", "broad"], how="left")

    low_completed_unsold = merged["completed_unsold_ratio_pct"].fillna(0) <= 0.08
    merged["정책후판정"] = "정책 중립"
    merged.loc[
        (merged["심화행동라벨"] == "우선 매수 검토")
        & (merged["임차수요기반양호"] == "예")
        & (merged["월세화경계"] != "예")
        & low_completed_unsold,
        "정책후판정",
    ] = "우선 회복 후보"
    merged.loc[
        (merged["정책후판정"] == "정책 중립")
        & (merged["심화행동라벨"].isin(["관찰 유지", "보수 접근"]))
        & ((merged["임차수요기반양호"] == "예") | (merged["임차선행"] == "예"))
        & (merged["월세화경계"] != "예")
        & low_completed_unsold,
        "정책후판정",
    ] = "정책 민감 관찰 후보"
    merged.loc[
        (merged["정책후판정"] == "정책 중립")
        & (
            (merged["월세화경계"] == "예")
            | merged["정책메모"].fillna("").str.contains("공급 부담")
            | (~low_completed_unsold)
            | (merged["심화행동라벨"] == "회피")
        ),
        "정책후판정",
    ] = "정책 리스크 경계"

    def build_note(row: pd.Series) -> str:
        parts: list[str] = []
        if pd.notna(row.get("completed_unsold_ratio_pct")):
            parts.append(f"준공후미분양비율 {row['completed_unsold_ratio_pct']:.3f}%")
        if isinstance(row.get("정책메모"), str) and row["정책메모"]:
            parts.append(row["정책메모"])
        if row.get("임차수요기반양호") == "예":
            parts.append("임차 수요 기반 양호")
        if row.get("임차선행") == "예":
            parts.append("임차 선행")
        if row.get("월세화경계") == "예":
            parts.append("월세화 경계")
        return ", ".join(parts)

    merged["정책판정메모"] = merged.apply(build_note, axis=1)
    columns = [
        "정책후판정",
        "심화행동라벨",
        "시도",
        "시군구",
        "투자검토분류_sale",
        "상승확률점수_cand",
        "투자적합성점수_cand",
        "정책메모",
        "임차수요기반양호",
        "월세화경계",
        "임차선행",
        "broad",
        "completed_unsold",
        "completed_unsold_ratio_pct",
        "정책판정메모",
    ]
    out = merged[columns].rename(
        columns={
            "투자검토분류_sale": "투자검토분류",
            "상승확률점수_cand": "상승확률점수",
            "투자적합성점수_cand": "투자적합성점수",
        }
    )
    return out.sort_values(
        ["정책후판정", "상승확률점수", "투자적합성점수"],
        ascending=[True, False, False],
    ).reset_index(drop=True)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH), read_only=True)
    grade = pd.read_csv(GRADE_PATH, encoding="utf-8-sig")
    monthly = build_monthly_metrics(con)

    sudogwon_top = region_tuples(grade.loc[grade["급지그룹"] == "상급지", ["시도", "시군구"]])
    sudogwon_other = region_tuples(grade.loc[grade["급지그룹"] != "상급지", ["시도", "시군구"]])
    core_tohuga = {
        ("서울특별시", "강남구"),
        ("서울특별시", "서초구"),
        ("서울특별시", "송파구"),
        ("서울특별시", "용산구"),
    }
    seoul_other = region_tuples(
        grade.loc[
            (grade["시도"] == "서울특별시") & (~grade["시군구"].isin(["강남구", "서초구", "송파구", "용산구"])),
            ["시도", "시군구"],
        ]
    )

    match_202503, selected_202503 = match_secondary_controls(
        monthly=monthly,
        grade=grade,
        event_ym=202503,
        treatment=core_tohuga,
        universe_filter=lambda df: (df["시도"] == "서울특별시")
        & (~df[["시도", "시군구"]].apply(tuple, axis=1).isin(core_tohuga)),
        label="2025-03 토허확대",
        top_n=4,
    )
    match_202507, selected_202507 = match_secondary_controls(
        monthly=monthly,
        grade=grade,
        event_ym=202507,
        treatment=core_tohuga,
        universe_filter=lambda df: (df["시도"] == "서울특별시")
        & (df["급지그룹"] == "상급지")
        & (~df[["시도", "시군구"]].apply(tuple, axis=1).isin(core_tohuga)),
        label="2025-07 대출규제",
        top_n=4,
    )

    control_frame = pd.concat([match_202503, match_202507], ignore_index=True)
    control_frame.to_csv(CONTROL_OUT, index=False, encoding="utf-8-sig")

    window_rows = [
        summarize_group(monthly, "2023-01 규제완화", "1순위 광역비교", "처리군: 수도권 상급지", 202301, sudogwon_top),
        summarize_group(monthly, "2023-01 규제완화", "1순위 광역비교", "대조군1: 수도권 중급지·하급지", 202301, sudogwon_other),
        summarize_group(monthly, "2025-03 토허확대", "1순위 권역비교", "처리군: 강남·서초·송파·용산", 202503, core_tohuga),
        summarize_group(monthly, "2025-03 토허확대", "1순위 권역비교", "대조군1: 서울 기타", 202503, seoul_other),
        summarize_group(monthly, "2025-03 토허확대", "2순위 유사상급지", "대조군2: 성동·마포·광진·동작", 202503, selected_202503),
        summarize_group(monthly, "2025-07 대출규제", "1순위 광역비교", "처리군: 수도권 상급지", 202507, sudogwon_top),
        summarize_group(monthly, "2025-07 대출규제", "1순위 광역비교", "대조군1: 수도권 중급지·하급지", 202507, sudogwon_other),
        summarize_group(monthly, "2025-07 대출규제", "2순위 유사상급지", "처리군: 강남·서초·송파·용산", 202507, core_tohuga),
        summarize_group(monthly, "2025-07 대출규제", "2순위 유사상급지", "대조군2: 성동·마포·광진·동작", 202507, selected_202507),
    ]
    pd.DataFrame(window_rows).to_csv(WINDOW_OUT, index=False, encoding="utf-8-sig")

    build_tohuga_context(con).to_csv(TOHUGA_OUT, index=False, encoding="utf-8-sig")
    build_recovery_candidates(con).to_csv(RECOVERY_OUT, index=False, encoding="utf-8-sig")

    for path in [WINDOW_OUT, CONTROL_OUT, TOHUGA_OUT, RECOVERY_OUT]:
        print(f"Wrote {path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
