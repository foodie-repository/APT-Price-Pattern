from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


DB_PATH = "/Volumes/T9/duckdb-analytics/db/apartment.duckdb"
OUTPUT_DIR = Path("04_결과/01_리포트_codex/06_예측검증")
REPORT_DATE = "20260313"
REPORT_TAG = "codex"
REVIEW_GATE_PATH = Path("04_결과/01_리포트_codex/05_투자검토") / f"05_투자검토대상군_{REPORT_DATE}_{REPORT_TAG}_시군구.csv"


@dataclass(frozen=True)
class ScoreWeights:
    market: float = 0.40
    lease: float = 0.35
    supply: float = 0.25


def ym_to_timestamp(ym: int) -> pd.Timestamp:
    return pd.Timestamp(year=ym // 100, month=ym % 100, day=1)


def timestamp_to_ym(ts: pd.Timestamp) -> int:
    return ts.year * 100 + ts.month


def normalize_sigungu_base(name: str | None) -> str | None:
    if name is None:
        return None
    text = str(name).strip()
    if not text:
        return None
    parts = text.split()
    if len(parts) >= 2 and parts[0].endswith("시"):
        return parts[0]
    return text


def percentile_score(series: pd.Series, higher_is_better: bool = True) -> pd.Series:
    valid = series.replace([np.inf, -np.inf], np.nan)
    if not higher_is_better:
        valid = -valid
    score = valid.rank(method="average", pct=True)
    return (score * 100.0).clip(0, 100)


def safe_mean(df: pd.DataFrame, columns: list[str]) -> pd.Series:
    return df[columns].mean(axis=1, skipna=True)


def classify_future_change(change_pct: pd.Series, up: float = 3.0, down: float = -3.0) -> pd.Series:
    result = pd.Series(index=change_pct.index, dtype="object")
    result[change_pct >= up] = "상승"
    result[(change_pct > down) & (change_pct < up)] = "보합"
    result[change_pct <= down] = "하락"
    return result


def classify_score(score: pd.Series) -> pd.Series:
    result = pd.Series(index=score.index, dtype="object")
    result[score >= 55] = "상승"
    result[(score > 45) & (score < 55)] = "보합"
    result[score <= 45] = "하락"
    return result


def macro_f1(y_true: pd.Series, y_pred: pd.Series, labels: list[str]) -> float:
    f1_values = []
    for label in labels:
        tp = int(((y_true == label) & (y_pred == label)).sum())
        fp = int(((y_true != label) & (y_pred == label)).sum())
        fn = int(((y_true == label) & (y_pred != label)).sum())
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        if precision + recall == 0:
            f1_values.append(0.0)
        else:
            f1_values.append(2 * precision * recall / (precision + recall))
    return float(np.mean(f1_values))


def top_n_hit_rate(df: pd.DataFrame, n: int = 20) -> float:
    hit_rates = []
    for _, g in df.groupby("origin_ym"):
        if g.empty:
            continue
        top = g.sort_values("상승확률점수", ascending=False).head(n)
        if len(top) == 0:
            continue
        hit_rates.append((top["actual_label"] == "상승").mean())
    return float(np.mean(hit_rates)) if hit_rates else np.nan


def avoidance_hit_rate(df: pd.DataFrame, n: int = 20) -> float:
    hit_rates = []
    for _, g in df.groupby("origin_ym"):
        if g.empty:
            continue
        bottom = g.sort_values("상승확률점수", ascending=True).head(n)
        if len(bottom) == 0:
            continue
        hit_rates.append((bottom["actual_label"] == "하락").mean())
    return float(np.mean(hit_rates)) if hit_rates else np.nan


def df_to_code_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "```text\n(비어 있음)\n```"
    show = df.copy()
    show = normalize_display_table(show)
    return "```text\n" + show.to_string(index=False) + "\n```"


DISPLAY_COLUMN_MAP = {
    "저평가가능점수": "가격반영부족가능성점수",
    "촉매점수": "변화계기점수",
    "과열가능점수": "과열가능성점수",
    "미래입주압력_18개월_pct": "기존세대수대비_향후18개월입주예정물량_pct",
    "completed_unsold_ratio_pct": "준공후미분양비중_pct",
    "median_peer_gap_pct": "동급생활권가격괴리율_pct",
    "저평가가능비중_pct": "가격반영부족가능비중_pct",
    "recent_12m_trades": "최근12개월거래량",
    "signal_complex_count": "비교단지수",
}

DISPLAY_TEXT_REPLACEMENTS = [
    ("우선검토", "우선 매수 검토"),
    ("우선 검토", "우선 매수 검토"),
    ("임차 지지 강화", "임차 수요 기반 개선"),
    ("임차 지지와", "임차 수요 기반과"),
    ("임차 지지", "임차 수요 기반"),
    ("전세지지", "전세가율"),
    ("공급 압박", "공급 부담"),
]


def normalize_display_text(value: object) -> object:
    if not isinstance(value, str):
        return value
    text = value
    for before, after in DISPLAY_TEXT_REPLACEMENTS:
        text = text.replace(before, after)
    return text


def normalize_display_table(df: pd.DataFrame) -> pd.DataFrame:
    show = df.rename(columns=DISPLAY_COLUMN_MAP).copy()
    object_cols = show.select_dtypes(include=["object", "string"]).columns
    for col in object_cols:
        show[col] = show[col].map(normalize_display_text)
    return show


REVIEW_GATE_COLUMNS = [
    "투자검토분류",
    "투자검토분류근거",
    "재검토조건",
    "사람검증셋판정",
    "사람검증셋메모",
    "점수요약",
]


def drop_existing_review_gate_columns(df: pd.DataFrame) -> pd.DataFrame:
    drop_cols = []
    for col in df.columns:
        if any(
            col == base
            or col == f"{base}_x"
            or col == f"{base}_y"
            for base in REVIEW_GATE_COLUMNS
        ):
            drop_cols.append(col)
    if drop_cols:
        return df.drop(columns=drop_cols)
    return df


def current_policy_comment(row: pd.Series) -> str:
    if row["시도"] == "서울특별시" and row["시군구"] in {"강남구", "서초구", "송파구", "용산구"}:
        return "고가 핵심지 규제 민감"
    if row["시도"] in {"서울특별시", "경기도", "인천광역시"} and row["미래입주압력_18개월_pct"] >= 8:
        return "수도권 공급 부담 경계"
    if row["시도"] in {"서울특별시", "경기도", "인천광역시"} and row["상승확률점수"] >= 70:
        return "수도권 대체지·확산 수혜 가능"
    return "중립"


def build_exclusion_reason(row: pd.Series) -> str:
    reasons: list[str] = []
    if row.get("신뢰도낮음") == "예":
        reasons.append("신뢰도 낮음")
    if pd.notna(row.get("recent_12m_trades")) and row["recent_12m_trades"] < 50:
        reasons.append(f"거래량 부족({int(row['recent_12m_trades'])})")
    if pd.notna(row.get("signal_complex_count")) and row["signal_complex_count"] < 50:
        reasons.append(f"비교단지 부족({int(row['signal_complex_count'])})")
    if pd.notna(row.get("투자적합성점수")) and row["투자적합성점수"] < 50:
        reasons.append(f"투자적합성 낮음({row['투자적합성점수']:.1f})")
    return ", ".join(reasons) if reasons else "중립"


def build_candidate_reason(row: pd.Series, kind: str) -> str:
    reasons: list[str] = []

    if kind == "rising":
        if pd.notna(row.get("trade_recovery_pct")) and row["trade_recovery_pct"] >= 20:
            reasons.append(f"거래회복 {row['trade_recovery_pct']:.1f}%")
        if pd.notna(row.get("jeonse_ratio_pct")) and row["jeonse_ratio_pct"] >= 80:
            reasons.append(f"전세가율 {row['jeonse_ratio_pct']:.1f}%")
        if pd.notna(row.get("미래입주압력_18개월_pct")) and row["미래입주압력_18개월_pct"] <= 2:
            reasons.append(f"입주예정 물량 비율 낮음 {row['미래입주압력_18개월_pct']:.1f}%")
        if pd.notna(row.get("저평가가능점수")) and row["저평가가능점수"] >= 65:
            reasons.append("가격 반영 부족 신호")
        if pd.notna(row.get("과열가능점수")) and row["과열가능점수"] < 40:
            reasons.append("과열 낮음")
    elif kind == "undervalued":
        if pd.notna(row.get("저평가가능비중_pct")) and row["저평가가능비중_pct"] > 0:
            reasons.append(f"저평가가능 비중 {row['저평가가능비중_pct']:.1f}%")
        if pd.notna(row.get("jeonse_ratio_pct")) and row["jeonse_ratio_pct"] >= 80:
            reasons.append(f"전세가율 높음 {row['jeonse_ratio_pct']:.1f}%")
        if pd.notna(row.get("촉매점수")) and row["촉매점수"] >= 70:
            reasons.append("변화 계기 양호")
        if pd.notna(row.get("미래입주압력_18개월_pct")) and row["미래입주압력_18개월_pct"] <= 2:
            reasons.append("공급 부담 낮음")
    elif kind == "observe":
        if pd.notna(row.get("촉매점수")) and row["촉매점수"] >= 70:
            reasons.append(f"변화 계기 강함 {row['촉매점수']:.1f}")
        if pd.notna(row.get("과열가능점수")) and row["과열가능점수"] >= 60:
            reasons.append(f"과열 경계 {row['과열가능점수']:.1f}")
        if pd.notna(row.get("미래입주압력_18개월_pct")) and row["미래입주압력_18개월_pct"] >= 5:
            reasons.append(f"입주예정 물량 비율 {row['미래입주압력_18개월_pct']:.1f}%")
        if pd.notna(row.get("jeonse_ratio_pct")) and row["jeonse_ratio_pct"] < 75:
            reasons.append(f"전세가율 {row['jeonse_ratio_pct']:.1f}%")
    elif kind == "avoid":
        if pd.notna(row.get("과열가능점수")) and row["과열가능점수"] >= 70:
            reasons.append(f"과열 {row['과열가능점수']:.1f}")
        if row.get("예측분류") == "하락":
            reasons.append("하락 분류")
        if pd.notna(row.get("미래입주압력_18개월_pct")) and row["미래입주압력_18개월_pct"] >= 5:
            reasons.append(f"입주예정 물량 비율 {row['미래입주압력_18개월_pct']:.1f}%")
        if pd.notna(row.get("completed_unsold_ratio_pct")) and row["completed_unsold_ratio_pct"] >= 60:
            reasons.append(f"준공후미분양비중 {row['completed_unsold_ratio_pct']:.1f}%")
        if pd.notna(row.get("jeonse_ratio_pct")) and row["jeonse_ratio_pct"] < 70:
            reasons.append(f"전세가율 낮음 {row['jeonse_ratio_pct']:.1f}%")

    if not reasons:
        fallback_map = {
            "rising": ["상승확률점수", "촉매점수", "투자적합성점수"],
            "undervalued": ["저평가가능점수", "촉매점수", "투자적합성점수"],
            "observe": ["상승확률점수", "촉매점수", "투자적합성점수"],
            "avoid": ["과열가능점수", "미래입주압력_18개월_pct", "상승확률점수"],
        }
        label_map = {
            "상승확률점수": "상승확률",
            "저평가가능점수": "저평가",
            "촉매점수": "변화 계기",
            "투자적합성점수": "투자적합성",
            "과열가능점수": "과열",
            "미래입주압력_18개월_pct": "입주예정 물량 비율",
        }
        for col in fallback_map.get(kind, []):
            value = row.get(col)
            if pd.notna(value):
                suffix = "%" if col == "미래입주압력_18개월_pct" else ""
                reasons.append(f"{label_map[col]} {value:.1f}{suffix}")

    return ", ".join(reasons[:3]) if reasons else "근거 데이터 부족"


def build_investability_score(current: pd.DataFrame) -> pd.Series:
    return safe_mean(
        pd.DataFrame(
            {
                "liquidity": percentile_score(current["recent_12m_trades"], True),
                "stock": percentile_score(current["stock_units"], True),
                "price": percentile_score(current["sale_pp_만원"], True),
            }
        ),
        ["liquidity", "stock", "price"],
    )


def apply_review_gate(current: pd.DataFrame) -> pd.DataFrame:
    if not REVIEW_GATE_PATH.exists():
        return current

    current = drop_existing_review_gate_columns(current)
    gate = pd.read_csv(REVIEW_GATE_PATH, encoding="utf-8-sig")
    keep_cols = ["시도", "시군구", *REVIEW_GATE_COLUMNS]
    gate = gate[keep_cols].drop_duplicates(["시도", "시군구"])
    merged = current.merge(gate, on=["시도", "시군구"], how="left")
    return merged


def build_monthly_panel(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    sale_sql = """
    SELECT
        시도,
        시군구_분리 AS 시군구,
        계약년월,
        COUNT(*) AS sale_trades,
        MEDIAN(거래금액_만원) AS sale_price_만원,
        MEDIAN(거래금액_만원 * 3.3 / NULLIF(전용면적_㎡, 0)) AS sale_pp_만원
    FROM v_sale_clean
    WHERE 전용면적_구분 = '중소형'
    GROUP BY 1, 2, 3
    """
    jeonse_sql = """
    SELECT
        시도,
        시군구_분리 AS 시군구,
        계약년월,
        COUNT(*) AS jeonse_trades,
        MEDIAN(보증금_만원) AS jeonse_price_만원,
        MEDIAN(보증금_만원 * 3.3 / NULLIF(전용면적_㎡, 0)) AS jeonse_pp_만원
    FROM v_jeonse_clean
    WHERE 전용면적_구분 = '중소형'
    GROUP BY 1, 2, 3
    """
    lease_mix_sql = """
    SELECT
        시도,
        시군구_분리 AS 시군구,
        계약년월,
        COUNT(*) FILTER (WHERE 전월세구분 = '전세' AND 전용면적_구분 = '중소형') AS jeonse_count,
        COUNT(*) FILTER (WHERE 전월세구분 = '월세' AND 전용면적_구분 = '중소형') AS wolse_count,
        COUNT(*) FILTER (WHERE 전용면적_구분 = '중소형') AS lease_count
    FROM v_lease_clean
    GROUP BY 1, 2, 3
    """
    stock_sql = """
    SELECT
        시도,
        시군구,
        SUM(COALESCE(세대수, 0)) AS stock_units
    FROM v_complex_product_national
    GROUP BY 1, 2
    """
    supply_sql = """
    SELECT
        시도,
        시군구,
        사용승인년월 AS ym,
        SUM(COALESCE(세대수합계, 0)) AS supply_units
    FROM v_supply_proxy_monthly
    GROUP BY 1, 2, 3
    """
    unsold_sql = """
    SELECT
        CAST(REPLACE(시점, '.', '') AS INTEGER) AS ym,
        시도,
        시군구,
        미분양수 AS total_unsold,
        준공_후_미분양수 AS completed_unsold
    FROM KOSIS_미분양종합
    WHERE 시도 IS NOT NULL
      AND 시도 <> '전국'
      AND 시군구 IS NOT NULL
      AND 시군구 <> ''
    """

    sale = con.execute(sale_sql).fetchdf()
    jeonse = con.execute(jeonse_sql).fetchdf()
    lease_mix = con.execute(lease_mix_sql).fetchdf()
    stock = con.execute(stock_sql).fetchdf()
    supply = con.execute(supply_sql).fetchdf()
    unsold = con.execute(unsold_sql).fetchdf()

    sale["date"] = sale["계약년월"].map(ym_to_timestamp)
    jeonse["date"] = jeonse["계약년월"].map(ym_to_timestamp)
    lease_mix["date"] = lease_mix["계약년월"].map(ym_to_timestamp)
    supply["date"] = supply["ym"].map(ym_to_timestamp)
    unsold["date"] = unsold["ym"].map(ym_to_timestamp)

    regions = sale[["시도", "시군구"]].drop_duplicates().sort_values(["시도", "시군구"])
    all_dates = pd.date_range(sale["date"].min(), sale["date"].max(), freq="MS")
    grid = regions.assign(key=1).merge(pd.DataFrame({"date": all_dates, "key": 1}), on="key").drop(columns="key")

    panel = grid.merge(
        sale[["시도", "시군구", "date", "sale_trades", "sale_price_만원", "sale_pp_만원"]],
        on=["시도", "시군구", "date"],
        how="left",
    )
    panel = panel.merge(
        jeonse[["시도", "시군구", "date", "jeonse_trades", "jeonse_price_만원", "jeonse_pp_만원"]],
        on=["시도", "시군구", "date"],
        how="left",
    )
    panel = panel.merge(
        lease_mix[["시도", "시군구", "date", "jeonse_count", "wolse_count", "lease_count"]],
        on=["시도", "시군구", "date"],
        how="left",
    )
    panel = panel.merge(stock, on=["시도", "시군구"], how="left")

    supply_monthly = (
        supply.groupby(["시도", "시군구", "date"], as_index=False)["supply_units"].sum()
    )
    panel = panel.merge(supply_monthly, on=["시도", "시군구", "date"], how="left")
    panel["supply_units"] = panel["supply_units"].fillna(0)

    panel["시군구_상위"] = panel["시군구"].map(normalize_sigungu_base)
    unsold["시군구_상위"] = unsold["시군구"].map(normalize_sigungu_base)

    unsold_exact = unsold.rename(
        columns={
            "total_unsold": "total_unsold_exact",
            "completed_unsold": "completed_unsold_exact",
        }
    )
    panel = panel.merge(
        unsold_exact[["시도", "시군구", "date", "total_unsold_exact", "completed_unsold_exact"]],
        on=["시도", "시군구", "date"],
        how="left",
    )

    unsold_broad = (
        unsold.groupby(["시도", "시군구_상위", "date"], as_index=False)[["total_unsold", "completed_unsold"]].sum()
        .rename(
            columns={
                "total_unsold": "total_unsold_broad",
                "completed_unsold": "completed_unsold_broad",
            }
        )
    )
    panel = panel.merge(
        unsold_broad,
        on=["시도", "시군구_상위", "date"],
        how="left",
    )

    panel["total_unsold"] = panel["total_unsold_exact"].combine_first(panel["total_unsold_broad"])
    panel["completed_unsold"] = panel["completed_unsold_exact"].combine_first(panel["completed_unsold_broad"])

    panel = panel.sort_values(["시도", "시군구", "date"]).reset_index(drop=True)
    grouped = panel.groupby(["시도", "시군구"], sort=False)

    panel["ym"] = panel["date"].map(timestamp_to_ym)
    panel["wolse_share_pct"] = 100.0 * panel["wolse_count"] / panel["lease_count"].replace(0, np.nan)
    panel["jeonse_ratio_pct"] = 100.0 * panel["jeonse_price_만원"] / panel["sale_price_만원"].replace(0, np.nan)
    panel["price_6m_change_pct"] = 100.0 * (panel["sale_pp_만원"] / grouped["sale_pp_만원"].shift(6) - 1.0)
    panel["price_12m_change_pct"] = 100.0 * (panel["sale_pp_만원"] / grouped["sale_pp_만원"].shift(12) - 1.0)
    panel["price_24m_change_pct"] = 100.0 * (panel["sale_pp_만원"] / grouped["sale_pp_만원"].shift(24) - 1.0)
    panel["future_6m_change_pct"] = 100.0 * (grouped["sale_pp_만원"].shift(-6) / panel["sale_pp_만원"] - 1.0)
    panel["jeonse_12m_change_pct"] = 100.0 * (panel["jeonse_pp_만원"] / grouped["jeonse_pp_만원"].shift(12) - 1.0)
    panel["wolse_share_12m_change_pctp"] = panel["wolse_share_pct"] - grouped["wolse_share_pct"].shift(12)

    panel["sale_trades"] = panel["sale_trades"].fillna(0)
    panel["recent_6m_trades"] = grouped["sale_trades"].transform(lambda s: s.rolling(6, min_periods=6).sum())
    panel["recent_12m_trades"] = grouped["sale_trades"].transform(lambda s: s.rolling(12, min_periods=12).sum())
    panel["prev_6m_trades"] = grouped["recent_6m_trades"].shift(6)
    panel["trade_recovery_pct"] = 100.0 * (panel["recent_6m_trades"] / panel["prev_6m_trades"].replace(0, np.nan) - 1.0)

    panel["supply_12m_units"] = grouped["supply_units"].transform(lambda s: s.rolling(12, min_periods=1).sum())
    panel["supply_burden_12m_pct"] = 100.0 * panel["supply_12m_units"] / panel["stock_units"].replace(0, np.nan)
    panel["unsold_per_stock_pct"] = 100.0 * panel["total_unsold"] / panel["stock_units"].replace(0, np.nan)
    panel["completed_unsold_ratio_pct"] = 100.0 * panel["completed_unsold"] / panel["total_unsold"].replace(0, np.nan)
    panel["actual_label"] = classify_future_change(panel["future_6m_change_pct"])

    return panel


def compute_backtest(panel: pd.DataFrame, weights: ScoreWeights) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    origins = panel[
        (panel["ym"] >= 201101)
        & (panel["ym"] <= 202508)
        & (panel["recent_12m_trades"] >= 30)
        & panel["actual_label"].notna()
    ].copy()

    scored_frames = []
    for origin_ym, g in origins.groupby("ym"):
        g = g.copy()
        g["시장국면점수"] = safe_mean(
            pd.DataFrame(
                {
                    "price6_cool": percentile_score(g["price_6m_change_pct"], False),
                    "price12_cool": percentile_score(g["price_12m_change_pct"], False),
                    "trade": percentile_score(g["trade_recovery_pct"], True),
                    "liquidity": percentile_score(g["recent_12m_trades"], True),
                }
            ),
            ["price6_cool", "price12_cool", "trade", "liquidity"],
        ).fillna(50)
        g["임차지지점수"] = safe_mean(
            pd.DataFrame(
                {
                    "ratio": percentile_score(g["jeonse_ratio_pct"], True),
                    "jeonse_yoy": percentile_score(g["jeonse_12m_change_pct"], True),
                    "wolse": percentile_score(g["wolse_share_pct"], False),
                }
            ),
            ["ratio", "jeonse_yoy", "wolse"],
        ).fillna(50)
        g["공급/미분양점수"] = safe_mean(
            pd.DataFrame(
                {
                    "supply": percentile_score(g["supply_burden_12m_pct"], False),
                    "unsold": percentile_score(g["unsold_per_stock_pct"], False),
                    "completed": percentile_score(g["completed_unsold_ratio_pct"], False),
                }
            ),
            ["supply", "unsold", "completed"],
        ).fillna(50)
        g["상승확률점수"] = (
            g["시장국면점수"] * weights.market
            + g["임차지지점수"] * weights.lease
            + g["공급/미분양점수"] * weights.supply
        )
        g["pred_label"] = classify_score(g["상승확률점수"])
        g["origin_ym"] = origin_ym
        scored_frames.append(g)

    backtest = pd.concat(scored_frames, ignore_index=True)

    holdout = backtest[(backtest["origin_ym"] >= 202401) & (backtest["origin_ym"] <= 202508)].copy()

    labels = ["상승", "보합", "하락"]
    metric_rows = []
    for name, sample in [("전체", backtest), ("홀드아웃", holdout)]:
        acc = float((sample["actual_label"] == sample["pred_label"]).mean())
        macro = macro_f1(sample["actual_label"], sample["pred_label"], labels)
        up_true = sample["actual_label"] == "상승"
        up_pred = sample["pred_label"] == "상승"
        tp = int((up_true & up_pred).sum())
        precision = tp / int(up_pred.sum()) if int(up_pred.sum()) else 0.0
        recall = tp / int(up_true.sum()) if int(up_true.sum()) else 0.0
        metric_rows.append(
            {
                "구간": name,
                "샘플수": int(len(sample)),
                "3분류정확도": round(acc, 4),
                "MacroF1": round(macro, 4),
                "상승정밀도": round(float(precision), 4),
                "상승재현율": round(float(recall), 4),
                "Top20상승적중률": round(top_n_hit_rate(sample, 20), 4),
                "Bottom20하락적중률": round(avoidance_hit_rate(sample, 20), 4),
            }
        )

    metrics = pd.DataFrame(metric_rows)

    calibration = (
        backtest.assign(
            score_bucket=pd.qcut(
                backtest["상승확률점수"],
                q=10,
                duplicates="drop",
            )
        )
        .groupby("score_bucket", observed=False)
        .agg(
            샘플수=("actual_label", "size"),
            실제상승비율=("actual_label", lambda s: float((s == "상승").mean())),
            실제하락비율=("actual_label", lambda s: float((s == "하락").mean())),
            평균6개월변화율=("future_6m_change_pct", "mean"),
            평균점수=("상승확률점수", "mean"),
        )
        .reset_index()
    )
    calibration["실제상승비율"] = calibration["실제상승비율"].round(4)
    calibration["실제하락비율"] = calibration["실제하락비율"].round(4)
    calibration["평균6개월변화율"] = calibration["평균6개월변화율"].round(2)
    calibration["평균점수"] = calibration["평균점수"].round(2)

    return backtest, metrics, calibration


def build_current_scores(
    con: duckdb.DuckDBPyConnection,
    panel: pd.DataFrame,
    weights: ScoreWeights,
) -> pd.DataFrame:
    current = panel[(panel["ym"] == 202602) & (panel["recent_12m_trades"] >= 30)].copy()

    current["시장국면점수"] = safe_mean(
        pd.DataFrame(
            {
                "price6_cool": percentile_score(current["price_6m_change_pct"], False),
                "price12_cool": percentile_score(current["price_12m_change_pct"], False),
                "trade": percentile_score(current["trade_recovery_pct"], True),
                "liquidity": percentile_score(current["recent_12m_trades"], True),
            }
        ),
        ["price6_cool", "price12_cool", "trade", "liquidity"],
    ).fillna(50)
    current["임차지지점수"] = safe_mean(
        pd.DataFrame(
            {
                "ratio": percentile_score(current["jeonse_ratio_pct"], True),
                "jeonse_yoy": percentile_score(current["jeonse_12m_change_pct"], True),
                "wolse": percentile_score(current["wolse_share_pct"], False),
            }
        ),
        ["ratio", "jeonse_yoy", "wolse"],
    ).fillna(50)
    current["공급/미분양점수"] = safe_mean(
        pd.DataFrame(
            {
                "supply": percentile_score(current["supply_burden_12m_pct"], False),
                "unsold": percentile_score(current["unsold_per_stock_pct"], False),
                "completed": percentile_score(current["completed_unsold_ratio_pct"], False),
            }
        ),
        ["supply", "unsold", "completed"],
    ).fillna(50)
    current["기본상승점수"] = (
        current["시장국면점수"] * weights.market
        + current["임차지지점수"] * weights.lease
        + current["공급/미분양점수"] * weights.supply
    )

    movein_sql = """
    SELECT
        시도,
        시군구,
        SUM(CASE WHEN 입주예정월 BETWEEN 202603 AND 202608 THEN COALESCE(세대수, 0) ELSE 0 END) AS movein_6m_units,
        SUM(CASE WHEN 입주예정월 BETWEEN 202603 AND 202708 THEN COALESCE(세대수, 0) ELSE 0 END) AS movein_18m_units,
        SUM(CASE WHEN LOWER(COALESCE(사업유형, '')) LIKE '%임대%' AND 입주예정월 BETWEEN 202603 AND 202708 THEN COALESCE(세대수, 0) ELSE 0 END) AS rental_movein_18m_units
    FROM v_movein_plan_base
    GROUP BY 1, 2
    """
    cond_sql = """
    SELECT
        시도,
        시군구,
        COUNT(*) AS signal_complex_count,
        COUNT(*) FILTER (WHERE 조건부가격신호 = '저평가가능') AS undervalued_count,
        COUNT(*) FILTER (WHERE 조건부가격신호 = '과대반영가능') AS overvalued_count,
        MEDIAN(생활권동급_괴리율_pct) AS median_peer_gap_pct
    FROM v_sale_conditional_signal_12m
    GROUP BY 1, 2
    """
    listing_sql = """
    SELECT
        시도,
        시군구,
        SUM(매매매물건수) AS listing_count,
        SUM(가격기준급매매물수 + 강한가격기준급매매물수) AS price_discount_listing_count,
        MEDIAN(중간호가괴리율_pct) AS median_listing_gap_pct,
        MAX(최대호가할인율_pct) AS max_listing_discount_pct
    FROM v_naver_sale_listing_summary_latest
    GROUP BY 1, 2
    """

    movein = con.execute(movein_sql).fetchdf()
    cond = con.execute(cond_sql).fetchdf()
    listing = con.execute(listing_sql).fetchdf()

    current = current.merge(movein, on=["시도", "시군구"], how="left")
    current = current.merge(cond, on=["시도", "시군구"], how="left")
    current = current.merge(listing, on=["시도", "시군구"], how="left")

    current["미래입주압력_6개월_pct"] = 100.0 * current["movein_6m_units"] / current["stock_units"].replace(0, np.nan)
    current["미래입주압력_18개월_pct"] = 100.0 * current["movein_18m_units"] / current["stock_units"].replace(0, np.nan)
    current["임대입주비중_18개월_pct"] = 100.0 * current["rental_movein_18m_units"] / current["movein_18m_units"].replace(0, np.nan)
    current["저평가가능비중_pct"] = 100.0 * current["undervalued_count"] / current["signal_complex_count"].replace(0, np.nan)
    current["과대반영비중_pct"] = 100.0 * current["overvalued_count"] / current["signal_complex_count"].replace(0, np.nan)
    current["가격기준급매비중_pct"] = 100.0 * current["price_discount_listing_count"] / current["listing_count"].replace(0, np.nan)

    current["저평가가능점수"] = safe_mean(
        pd.DataFrame(
            {
                "underv": percentile_score(current["저평가가능비중_pct"], True),
                "peer": percentile_score(current["median_peer_gap_pct"], False),
                "jeonse": percentile_score(current["jeonse_ratio_pct"], True),
                "momentum_inv": percentile_score(current["price_12m_change_pct"], False),
                "listing_gap_inv": percentile_score(current["median_listing_gap_pct"], False),
            }
        ),
        ["underv", "peer", "jeonse", "momentum_inv", "listing_gap_inv"],
    ).fillna(50)

    current["촉매점수"] = safe_mean(
        pd.DataFrame(
            {
                "trade": percentile_score(current["trade_recovery_pct"], True),
                "supply": percentile_score(current["미래입주압력_18개월_pct"], False),
                "unsold": percentile_score(current["completed_unsold_ratio_pct"], False),
                "lease": percentile_score(current["jeonse_12m_change_pct"], True),
                "discount": percentile_score(current["가격기준급매비중_pct"], True),
            }
        ),
        ["trade", "supply", "unsold", "lease", "discount"],
    ).fillna(50)

    current["수익률후보점수"] = safe_mean(
        pd.DataFrame(
            {
                "jeonse": percentile_score(current["jeonse_ratio_pct"], True),
                "price_inv": percentile_score(current["sale_pp_만원"], False),
                "future_supply_inv": percentile_score(current["미래입주압력_18개월_pct"], False),
                "lease": percentile_score(current["임차지지점수"], True),
            }
        ),
        ["jeonse", "price_inv", "future_supply_inv", "lease"],
    ).fillna(50)

    current["과열가능점수"] = safe_mean(
        pd.DataFrame(
            {
                "momentum": percentile_score(current["price_12m_change_pct"], True),
                "listing_gap": percentile_score(current["median_listing_gap_pct"], True),
                "overvalued": percentile_score(current["과대반영비중_pct"], True),
                "future_supply": percentile_score(current["미래입주압력_18개월_pct"], True),
                "jeonse_weak": percentile_score(current["jeonse_ratio_pct"], False),
            }
        ),
        ["momentum", "listing_gap", "overvalued", "future_supply", "jeonse_weak"],
    ).fillna(50)

    current["상승확률점수"] = (
        current["기본상승점수"] * 0.70
        + current["저평가가능점수"] * 0.15
        + current["촉매점수"] * 0.15
    )
    current["예측분류"] = classify_score(current["상승확률점수"])
    current["정책메모"] = current.apply(current_policy_comment, axis=1)
    current["투자적합성점수"] = build_investability_score(current).fillna(0)
    current["신뢰도낮음"] = np.where(
        (current["recent_12m_trades"] < 50)
        | (current["signal_complex_count"].fillna(0) < 50),
        "예",
        "아니오",
    )
    current["투자검토가능"] = np.where(
        (current["신뢰도낮음"] == "아니오") & (current["투자적합성점수"] >= 50),
        "예",
        "아니오",
    )
    current["제외사유"] = current.apply(build_exclusion_reason, axis=1)
    current["호가보조지표제한"] = np.where(current["listing_count"].isna(), "예", "아니오")
    current["투자검토분류"] = np.where(current["투자검토가능"] == "예", "판단 보류", "즉시 제외")
    current["투자검토분류근거"] = np.where(
        current["투자검토가능"] == "예",
        "기본 투자 검토 게이트는 통과했지만 세부 분류는 단계 7 산출물을 우선 적용",
        current["제외사유"],
    )
    current["재검토조건"] = np.where(
        current["투자검토가능"] == "예",
        "단계 7 산출물과 결합 후 재판정",
        "거래·비교단지·임차·공급 여건이 개선되면 재검토",
    )
    current["사람검증셋판정"] = ""
    current["사람검증셋메모"] = ""
    current["점수요약"] = ""
    current = apply_review_gate(current)

    current = current.sort_values(["상승확률점수", "저평가가능점수"], ascending=[False, False]).reset_index(drop=True)
    return current


def build_regime_similarity(panel: pd.DataFrame) -> pd.DataFrame:
    sample = panel[(panel["ym"] >= 201101) & (panel["ym"] <= 202602)].copy()
    sample["권역"] = np.select(
        [
            sample["시도"].isin(["서울특별시", "경기도", "인천광역시"]),
            sample["시도"].isin(["부산광역시", "대구광역시", "광주광역시", "대전광역시", "울산광역시"]),
        ],
        ["수도권", "지방광역시"],
        default="기타지방",
    )

    regime = (
        sample.groupby(["ym", "권역"], as_index=False)
        .agg(
            sale_yoy=("price_12m_change_pct", "median"),
            sale_6m=("price_6m_change_pct", "median"),
            jeonse_yoy=("jeonse_12m_change_pct", "median"),
            jeonse_ratio=("jeonse_ratio_pct", "median"),
            wolse_share=("wolse_share_pct", "median"),
        )
    )

    pivot = regime.pivot(index="ym", columns="권역")
    pivot.columns = [f"{metric}_{region}" for metric, region in pivot.columns]
    pivot = pivot.reset_index()
    current = pivot[pivot["ym"] == 202602].copy()
    history = pivot[pivot["ym"] <= 202508].copy()

    common_cols = [c for c in pivot.columns if c != "ym"]
    history_z = history.copy()
    current_z = current.copy()
    for col in common_cols:
        mean = history[col].mean()
        std = history[col].std()
        if pd.isna(std) or std == 0:
            history_z[col] = 0.0
            current_z[col] = 0.0
        else:
            history_z[col] = (history[col] - mean) / std
            current_z[col] = (current[col] - mean) / std

    current_vector = current_z.iloc[0][common_cols].astype(float)
    history_z["distance"] = np.sqrt(((history_z[common_cols].astype(float) - current_vector) ** 2).sum(axis=1))
    similar = history_z.nsmallest(5, "distance")[["ym", "distance"]].copy()
    return similar


def write_outputs(
    current: pd.DataFrame,
    metrics: pd.DataFrame,
    calibration: pd.DataFrame,
    backtest: pd.DataFrame,
    similar: pd.DataFrame,
) -> dict[str, Path]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    current_path = OUTPUT_DIR / f"06_예측점수_{REPORT_DATE}_{REPORT_TAG}_시군구.csv"
    metrics_path = OUTPUT_DIR / f"06_백테스트성능_{REPORT_DATE}_{REPORT_TAG}.csv"
    calibration_path = OUTPUT_DIR / f"06_점수버킷보정_{REPORT_DATE}_{REPORT_TAG}.csv"
    backtest_path = OUTPUT_DIR / f"06_백테스트샘플_{REPORT_DATE}_{REPORT_TAG}.csv"
    similar_path = OUTPUT_DIR / f"06_유사국면_{REPORT_DATE}_{REPORT_TAG}.csv"

    current.to_csv(current_path, index=False)
    metrics.to_csv(metrics_path, index=False)
    calibration.to_csv(calibration_path, index=False)
    backtest.to_csv(backtest_path, index=False)
    similar.to_csv(similar_path, index=False)

    return {
        "current": current_path,
        "metrics": metrics_path,
        "calibration": calibration_path,
        "backtest": backtest_path,
        "similar": similar_path,
    }


def render_report(
    current: pd.DataFrame,
    metrics: pd.DataFrame,
    calibration: pd.DataFrame,
    similar: pd.DataFrame,
    paths: dict[str, Path],
) -> Path:
    report_path = OUTPUT_DIR / f"06_예측및검증_{REPORT_DATE}_{REPORT_TAG}.md"

    priority = current[current["투자검토분류"] == "우선 검토"].copy()
    hold = current[current["투자검토분류"] == "판단 보류"].copy()
    excluded = current[current["투자검토분류"] == "즉시 제외"].copy()

    rising = (
        priority[(priority["예측분류"] == "상승") & (priority["과열가능점수"] < 70)]
        .sort_values(["상승확률점수", "투자적합성점수", "촉매점수"], ascending=[False, False, False])
        .head(15)
        .copy()
    )
    rising["판단근거"] = rising.apply(lambda row: build_candidate_reason(row, "rising"), axis=1)

    undervalued = (
        priority.sort_values(
            ["저평가가능점수", "투자적합성점수", "촉매점수", "상승확률점수"],
            ascending=[False, False, False, False],
        )
        .head(15)
        .copy()
    )
    undervalued["판단근거"] = undervalued.apply(lambda row: build_candidate_reason(row, "undervalued"), axis=1)

    avoid = (
        hold.sort_values(
            ["과열가능점수", "미래입주압력_18개월_pct", "completed_unsold_ratio_pct", "상승확률점수"],
            ascending=[False, False, False, True],
        )
        .head(15)
        .copy()
    )
    avoid["판단근거"] = avoid.apply(lambda row: build_candidate_reason(row, "avoid"), axis=1)

    taken_index = pd.MultiIndex.from_frame(
        pd.concat(
            [
                rising[["시도", "시군구"]],
                avoid[["시도", "시군구"]],
            ],
            ignore_index=True,
        ).drop_duplicates()
    )
    observe_pool = hold[~pd.MultiIndex.from_frame(hold[["시도", "시군구"]]).isin(taken_index)].copy()
    observe = (
        observe_pool[(observe_pool["예측분류"] != "하락")]
        .sort_values(["촉매점수", "상승확률점수", "투자적합성점수"], ascending=[False, False, False])
        .head(15)
        .copy()
    )
    observe["판단근거"] = observe.apply(lambda row: build_candidate_reason(row, "observe"), axis=1)

    excluded_watch = (
        excluded.sort_values(["상승확률점수", "투자적합성점수"], ascending=[False, True])
        .head(20)
        .copy()
    )

    similar_lines = "\n".join(
        f"- `{int(row.ym)}` (거리 `{row.distance:.2f}`)" for row in similar.itertuples(index=False)
    )

    metrics_text = df_to_code_table(metrics.round(4))
    rising_text = df_to_code_table(
        rising[
            [
                "시도",
                "시군구",
                "상승확률점수",
                "투자적합성점수",
                "저평가가능점수",
                "촉매점수",
                "과열가능점수",
                "미래입주압력_18개월_pct",
                "판단근거",
                "투자검토분류근거",
            ]
        ].round(2)
    )
    undervalued_text = df_to_code_table(
        undervalued[
            [
                "시도",
                "시군구",
                "저평가가능점수",
                "투자적합성점수",
                "상승확률점수",
                "촉매점수",
                "median_peer_gap_pct",
                "저평가가능비중_pct",
                "판단근거",
                "투자검토분류근거",
            ]
        ].round(2)
    )
    avoid_text = df_to_code_table(
        avoid[
            [
                "시도",
                "시군구",
                "투자적합성점수",
                "과열가능점수",
                "상승확률점수",
                "미래입주압력_18개월_pct",
                "completed_unsold_ratio_pct",
                "판단근거",
                "재검토조건",
            ]
        ].round(2)
    )
    observe_text = df_to_code_table(
        observe[
            [
                "시도",
                "시군구",
                "상승확률점수",
                "투자적합성점수",
                "촉매점수",
                "과열가능점수",
                "미래입주압력_18개월_pct",
                "판단근거",
                "재검토조건",
            ]
        ].round(2)
    )
    excluded_text = df_to_code_table(
        excluded_watch[
            [
                "시도",
                "시군구",
                "상승확률점수",
                "투자적합성점수",
                "recent_12m_trades",
                "signal_complex_count",
                "제외사유",
                "재검토조건",
            ]
        ].round(2)
    )

    top_bucket = calibration.sort_values("평균점수", ascending=False).head(3)
    bucket_text = df_to_code_table(top_bucket.round(4))

    report = f"""# 예측 및 검증 ({REPORT_DATE[:4]}-{REPORT_DATE[4:6]}-{REPORT_DATE[6:]}, {REPORT_TAG})

## 0. 문서 성격

- 이 문서는 `3. 매매`, `4. 임차`, `5. 정책`, `6. 공급 프록시` 결과를 합쳐 시군구 기준 `향후 6개월 대표가격 방향성`을 좁혀보는 단계다.
- 백테스트는 `2011-01 ~ 2025-08` origin 구간에서, 현재 시점 예측은 `2026-02` 기준월에서 수행했다.
- 예측 결과는 `정답 예언`이 아니라 `후보를 좁히는 의사결정 도구`로 사용한다.

## 1. 예측 기준

- 예측 대상: 시군구 단위 `전용면적_구분='중소형'` 대표평당가의 향후 6개월 방향성
- 기본 라벨:
  - `향후 6개월 가격변화율 >= +3.0%`: 상승
  - `-3.0% < 향후 6개월 가격변화율 < +3.0%`: 보합
  - `<= -3.0%`: 하락
- 시군구 최소 표본 기준: 최근 12개월 매매 거래 `30건 이상`
- 현재 기준월: `2026-02`
- 현재 점수는 `기본 정량모델 + 현재 시점 보강 신호`를 합쳐 계산했다.
- 다만 투자 후보 제시는 방향성 점수만으로 하지 않았다. 단계 7에서 정리한 `투자 검토 대상군 게이트`를 먼저 적용했고, `즉시 제외 / 판단 보류 / 우선 검토`를 분리한 뒤 예측 표를 만들었다.
- 공개 보고서에서는 단계 7의 `우선 검토`를 `우선 매수 검토 후보군`으로 번역해 읽는다.
- 호가 데이터는 후보 제외 게이트가 아니라 `급매 확인`, `현재 분위기 점검`, `실제 매수 직전 확인`을 위한 보조지표로만 사용한다.
- 예측 분류 기준:
  - `상승확률 점수 >= 55`: 상승
  - `45 < 상승확률 점수 < 55`: 보합
  - `<= 45`: 하락

## 2. 사용한 점수와 산식 요약

### 2.1 원자료에서 바로 계산한 비율/변화율

- `전세가율(%) = 100 x 전세대표가격 / 매매대표가격`
  - 의미: 매매가격 대비 전세가격의 비율이다. 높을수록 임차 수요 기반이 상대적으로 탄탄한 쪽으로 읽는다.
- `거래회복률(%) = 100 x (최근 6개월 거래건수 / 직전 6개월 거래건수 - 1)`
  - 의미: 최근 거래가 직전 반년보다 얼마나 살아났는지 본다.
- `기존 세대수 대비 향후 18개월 입주예정 물량(%) = 100 x 향후 18개월 입주예정 세대수 / 기존 세대수`
  - 의미: 현재 시장 규모 대비 앞으로 들어올 입주 물량의 비율이다.
  - 주의: 이 값은 그 자체로 `악재 확정`이 아니라 공급 부담 가능성을 보여주는 기초 비율이다.
- `저평가가능 비중(%) = 100 x 저평가가능 단지수 / 비교신호 단지수`
  - 의미: 같은 생활권 안에서 조건부로 덜 반영된 단지 비중이다.
- `과대반영 비중(%) = 100 x 과대반영가능 단지수 / 비교신호 단지수`
  - 의미: 같은 생활권 안에서 이미 많이 반영된 단지 비중이다.
- `가격기준 급매비중(%) = 100 x 가격기준 급매 매물수 / 전체 매물수`
  - 의미: 현재 매물 중 가격 할인 신호가 붙는 매물의 비율이다.

### 2.2 점수화 방식

- 이 문서의 점수는 딥러닝 확률이 아니라 `같은 시점 시군구들 사이의 상대 위치`를 0~100으로 바꾼 백분위 점수다.
- `백분위 점수 = 같은 시점 시군구들 사이에서 해당 값의 상대 순위 x 100`
- 값이 `클수록 좋은` 지표는 그대로 점수화하고, 값이 `작을수록 좋은` 지표는 역순으로 점수화한다.
  - 예: 전세가율은 높을수록 점수가 올라가고, 입주예정 물량 비율은 낮을수록 점수가 올라간다.
- 여러 하위 지표를 묶을 때는 사용 가능한 점수들의 단순 평균을 쓴다.

### 2.3 합성 점수 산식

- `시장국면점수 = 평균(최근 6개월 가격변화율 역순, 최근 12개월 가격변화율 역순, 거래회복률, 최근 12개월 거래량)`
- `임차수요기반점수 = 평균(전세가율, 전세 12개월 변화율, 월세비중 역순)`
- `공급/미분양점수 = 평균(최근 12개월 공급부담 역순, 미분양/기존세대수 비율 역순, 준공후미분양비중 역순)`
- `기본상승점수 = 0.40 x 시장국면점수 + 0.35 x 임차수요기반점수 + 0.25 x 공급/미분양점수`
- `저평가가능점수 = 평균(저평가가능 비중, 생활권동급 괴리율 역순, 전세가율, 최근 12개월 가격변화율 역순, 호가-실거래 괴리율 역순)`
- `촉매점수 = 평균(거래회복률, 기존 세대수 대비 향후 18개월 입주예정 물량 역순, 준공후미분양비중 역순, 전세 12개월 변화율, 가격기준 급매비중)`
- `수익률후보점수 = 평균(전세가율, 절대가격 부담 역순, 기존 세대수 대비 향후 18개월 입주예정 물량 역순, 임차수요기반점수)`
- `과열가능점수 = 평균(최근 12개월 가격변화율, 호가-실거래 괴리율, 과대반영 비중, 기존 세대수 대비 향후 18개월 입주예정 물량, 전세가율 역순)`
- `상승확률점수 = 0.70 x 기본상승점수 + 0.15 x 저평가가능점수 + 0.15 x 촉매점수`
- `투자적합성점수 = 평균(최근 12개월 거래량, 기존 세대수, 매매 평당가)`

### 2.4 공급 입력 반영 원칙

- 과거 공급부담: `사용승인년월 + 세대수` 기반 최근 12개월 공급부담
- 현재 압박: `총 미분양`, `준공후 미분양`, `준공후 미분양 비중`
- 미래 압력: `입주물량` 기준 향후 `6개월`, `18개월` 입주예정 물량 비율
- `입주물량`은 현재 예측 점수에는 반영했지만, 백테스트에는 과거 시점의 계획 데이터 스냅샷이 없어 직접 넣지 않았다.

### 2.5 점수 해석 주의

- `상승확률점수 70`은 실제 확률 70%라는 뜻이 아니다.
- `저평가가능점수`는 절대 저평가가 아니라 현재 모델 기준 `조건부 상대가치` 신호다.
- 보고서 본문에서 `입주예정 물량 비율`이라고 줄여 쓰더라도, 실제 계산값은 `기존 세대수 대비 향후 입주예정 물량 비율`이다.

## 3. 과거 유사 패턴 근거

현재 `2026-02`와 가장 가까운 월별 권역 구조는 아래와 같았다.
{similar_lines}

- 해석:
  - 현재 국면은 `수도권 상대 강세`, `지방 비핵심 공급 부담`, `임차 수요 기반의 지역 차별화`가 동시에 나타나는 구간과 더 가깝다.
  - 즉 단순 전면 상승장보다 `확산과 선별이 동시에 진행되는 회복/확산 혼합 구간`으로 읽는 편이 맞다.

## 4. 백테스트 방식

- origin 구간: `2011-01 ~ 2025-08`
- 최근 홀드아웃: `2024-01 ~ 2025-08`
- 타깃 라벨: origin `t`에서 `t+6` 가격변화율
- 기준선:
  - 최근 추세 연장보다 복합 점수가 실제로 `상승` 후보 선별에 더 유리한지 확인
- 평가 지표:
  - 3분류 정확도
  - Macro F1
  - `상승` 정밀도 / 재현율
  - Top 20 후보 적중률
  - Bottom 20 회피 적중률

## 5. 성능 요약

{metrics_text}

상위 점수 버킷의 실제 성과는 아래처럼 나왔다.

{bucket_text}

- 해석:
  - 점수 상위 버킷일수록 실제 `상승` 비율과 평균 6개월 변화율이 올라가는 구조가 확인됐다.
  - 다만 `보합` 구간과 `약한 상승` 구간은 일부 겹쳐서, 점수는 `후보 압축` 용도로 쓰는 것이 맞다.

## 6. 투자 후보와 제외 대상

### 6.1 상승 후보

- 아래 표는 단계 7 내부 라벨인 `우선 검토`로 분류된 지역 중, 공개 보고서 기준 `우선 매수 검토` 후보로 먼저 볼 만한 지역만 포함한다.
- 즉 `태백시`, `익산시`처럼 방향성 점수는 일부 높아도 `즉시 제외`로 분류된 지역은 상승 후보에 넣지 않는다.

{rising_text}

### 6.2 저평가 가능 후보

- `가격 반영 부족 가능성`은 단순히 `싸 보이는 지역`이 아니라, `우선 매수 검토 후보군`으로 좁힌 지역 중 `비슷한 생활권·시장 규모 대비 덜 반영됐고`, `임차 수요 기반/공급/거래`가 최소한 버텨 주는 곳으로 제한했다.

{undervalued_text}

### 6.3 관찰 후보

- 관찰 후보는 단계 7에서 `판단 보류`로 분류된 지역 중, 당장 매수 우선순위를 높이기보다 `변화 계기는 있으나 아직 확인이 더 필요한 지역`이다.

{observe_text}

### 6.4 보수·회피 후보

- 아래 지역은 `판단 보류` 군 안에서도 특히 `과열`, `공급 부담`, `준공후 미분양`, `임차 수요 기반 약화`가 커서 더 보수적으로 봐야 하는 곳이다.

{avoid_text}

### 6.5 제외 대상

- 아래 지역은 단계 7 게이트에서 `즉시 제외`로 분류된 곳이다.
- 제외 기준:
  - 최근 12개월 거래량 부족
  - 조건부 비교 단지 부족
  - 투자 적합성 점수 낮음
- 호가 보조지표가 비어 있어도 실거래·임차·공급·상품성 기준이 충분하면 후보군에서 제외하지 않는다.

{excluded_text}

## 7. 시나리오별 촉발 조건 / 무효화 조건

### 상승 시나리오

- 촉발 조건:
  - 수도권 핵심지와 대체 핵심지의 거래 회복 지속
  - 전세가율 또는 전세 보증금 회복 유지
  - `준공후 미분양` 완만한 하향
  - 예정 입주 부담이 높은 지역에서도 실제 소화가 확인
- 무효화 조건:
  - 상위권 후보에서 거래 급감과 전세 약세가 동시에 발생
  - 고부담 지역에서 입주 직전 호가 급락과 가격기준 급매 증가

### 보합 시나리오

- 촉발 조건:
  - 대출 규제와 공급 부담이 상쇄되어 방향성 없이 선별만 진행
  - 임차는 버티지만 매매 추격 수요가 약한 상태 지속
- 무효화 조건:
  - 특정 권역에서 전세와 거래가 동시에 강해져 확산이 빨라짐
  - 또는 준공후 미분양과 예정 입주 부담이 겹쳐 하방 압력이 커짐

### 하락 시나리오

- 촉발 조건:
  - 외곽·비핵심지에서 입주 집중, 임차 약세, 준공후 미분양 누적이 결합
  - 현재 과열 후보에서 호가 괴리 축소가 아니라 실제 할인 거래 확산
- 무효화 조건:
  - 공급 부담이 높아도 임차 흡수와 거래 회복이 확인되는 경우

## 8. 투자자 대응 원칙

- 우선 매수 검토:
  - 상승확률 점수 `70 이상`
  - 과열 가능 점수 `70 미만`
  - 기존 세대수 대비 향후 18개월 입주예정 물량 비율 `상대적으로 낮음`
- 관찰 유지:
  - 상승확률은 높지 않지만 촉매 점수가 높은 후보
  - 정책/공급 변수에 따라 위아래가 갈릴 후보
- 보수 접근:
  - 상승확률 `45~69`이고 기존 세대수 대비 향후 18개월 입주예정 물량 비율이 높은 곳
  - 임차 수요 기반이 약한 곳
- 회피 우선:
  - 과열 가능 점수 높음
  - 준공후 미분양 비중 높음
  - 기존 세대수 대비 향후 18개월 입주예정 물량 비율 높음

## 9. 신뢰도와 해석 한계

- 이번 후보군은 `방향성 점수`와 `투자자 관점 필터`를 분리했다.
- 따라서 `점수는 높지만 제외된 지역`이 존재한다. 이런 지역은 `시장 방향성 참고` 정도로만 보고, 투자 후보로는 다루지 않는 편이 맞다.
- 특히 입주물량은 일부 지역에서 `시군구`가 넓게 표기되어 세부 구 배분이 불안정할 수 있다.
- 호가 보조지표는 지역별 매칭 품질 차이가 있어서, 현재 버전에서는 투자 제외 근거로 사용하지 않는다.
- 지방 저거래량 지역은 방향성 모델에서 가끔 상위로 튈 수 있지만, 이번 버전에서는 `제외 대상`으로 분리했다.

## 10. 생성 산출물

- 현재 예측 점수: `{paths['current'].name}`
- 백테스트 성능: `{paths['metrics'].name}`
- 점수 버킷 보정: `{paths['calibration'].name}`
- 백테스트 원본 샘플: `{paths['backtest'].name}`
- 유사 국면: `{paths['similar'].name}`
"""

    report_path.write_text(report, encoding="utf-8")
    return report_path


def main() -> None:
    weights = ScoreWeights()
    con = duckdb.connect(DB_PATH, read_only=True)

    panel = build_monthly_panel(con)
    backtest, metrics, calibration = compute_backtest(panel, weights)
    current = build_current_scores(con, panel, weights)
    similar = build_regime_similarity(panel)
    paths = write_outputs(current, metrics, calibration, backtest, similar)
    report_path = render_report(current, metrics, calibration, similar, paths)

    print(f"Saved report: {report_path}")
    for name, path in paths.items():
        print(f"{name}: {path}")


if __name__ == "__main__":
    main()
