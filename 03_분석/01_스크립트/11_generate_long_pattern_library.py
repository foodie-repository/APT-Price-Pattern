from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import duckdb
import numpy as np
import pandas as pd


DB_PATH = "/Volumes/T9/duckdb-analytics/db/apartment.duckdb"
OUTPUT_DIR = Path("04_결과/01_리포트_codex/00_공통")
PHASE_SUMMARY_PATH = OUTPUT_DIR / "00_장기패턴_국면요약_20260314_codex.csv"
SIMILARITY_PATH = OUTPUT_DIR / "00_현재국면_과거유사국면비교_20260314_codex.csv"
REPORT_PATH = OUTPUT_DIR / "00_장기패턴라이브러리_20260314_codex.md"

SIMILARITY_FEATURES = [
    "수도권_평균YoY_pct",
    "지방광역시_평균YoY_pct",
    "기타지방_평균YoY_pct",
    "수도권_상승지역비중_pct",
    "지방광역시_상승지역비중_pct",
    "기타지방_상승지역비중_pct",
    "수도권_거래량지수",
    "지방광역시_거래량지수",
    "기타지방_거래량지수",
    "수도권_전세가율_pct",
    "지방광역시_전세가율_pct",
    "기타지방_전세가율_pct",
    "수도권_월세비중_pct",
    "지방광역시_월세비중_pct",
    "기타지방_월세비중_pct",
]


@dataclass(frozen=True)
class PhaseDefinition:
    order: int
    phase_id: str
    phase_name: str
    start_ym: int
    end_ym: int
    macro_tag: str
    policy_tag: str
    repeat_pattern: str
    exception_pattern: str
    note: str


PHASES: list[PhaseDefinition] = [
    PhaseDefinition(
        1,
        "phase_01",
        "전국 상승·버블 확대기",
        200601,
        200809,
        "전국 동조화 확산",
        "규제 강화 전후·유동성 확대",
        "수도권과 지방이 함께 오를 때는 확산 속도가 매우 넓고 빠르다.",
        "임차·월세 구조 데이터는 아직 부족해 매매 축 위주로만 해석해야 한다.",
        "전국적으로 동조화된 확산이 강했고, 수도권과 지방의 가격 상승률 차이가 크지 않았다.",
    ),
    PhaseDefinition(
        2,
        "phase_02",
        "금융위기 후 침체·전세부담 누적기",
        200810,
        201312,
        "전반 조정·핵심지 선행 반응",
        "금융위기·DTI 규제·보금자리 공급",
        "침체기에도 핵심지는 기사와 거래에서 가장 먼저 반응하고, 전세 부담은 누적된다.",
        "강남 등 일부 핵심지 움직임을 전체 상승장 신호로 오해하기 쉽다.",
        "전반 조정이 길었지만 전세 부담이 누적되면서 이후 회복기의 바닥을 형성한 구간이다.",
    ),
    PhaseDefinition(
        3,
        "phase_03",
        "규제완화 기반 회복기",
        201401,
        201612,
        "회복 초기·전세 선행",
        "양도세·청약제도 완화, LTV·DTI 완화",
        "전세가율 상승과 분양시장 반응이 매매 회복보다 먼저 나타난다.",
        "회복 초기에는 전체 시장보다 분양시장과 대체 핵심지에서 먼저 신호가 보인다.",
        "전세 부담 누적, 정책 완화, 공급 축소 신호가 겹치며 회복이 본격화된 구간이다.",
    ),
    PhaseDefinition(
        4,
        "phase_04",
        "수도권 선도·확산기",
        201701,
        201906,
        "수도권 선도 확산",
        "규제 강화와 풍선효과 병행",
        "상급지 규제 후에는 대체 핵심지와 인접 생활권으로 확산되는 패턴이 반복된다.",
        "좋은 단지와 좋은 지역이 먼저 움직이고, 외곽은 시차를 두고 따라온다.",
        "상급지 선도 회복이 마포·성동·분당·수지 등 대체 핵심지로 확산된 구간이다.",
    ),
    PhaseDefinition(
        5,
        "phase_05",
        "초저금리 급등·전국 동조화기",
        201907,
        202110,
        "전국 과열 확산",
        "초저금리·유동성 확대",
        "상승 말기에는 외곽, 저유동성, 저품질 자산까지 확산되며 과열 신호가 넓어진다.",
        "공시지가 1억 미만, 외곽 말단 지역 확산은 말기 과열 신호일 수 있다.",
        "전국 동조화와 과열이 강했고, 질이 낮은 자산까지 확산된 대표적 급등 구간이다.",
    ),
    PhaseDefinition(
        6,
        "phase_06",
        "금리충격·조정기",
        202111,
        202312,
        "전반 조정·고금리 압박",
        "금리 급등·대출 총량 규제",
        "고금리기에는 거래 급감과 매매 조정이 먼저 나타나고, 지방과 외곽의 약세가 더 깊다.",
        "같은 조정기라도 핵심지는 가격보다 거래 둔화로 먼저 반응하는 경우가 많다.",
        "금리 충격과 대출 규제로 전국 조정이 진행됐고, 지방과 외곽의 체력 차이가 커진 구간이다.",
    ),
    PhaseDefinition(
        7,
        "phase_07",
        "수도권 선도 회복기",
        202401,
        202506,
        "수도권 선도 회복",
        "규제완화 이후 선별 회복",
        "전세가율 회복과 거래 정상화가 수도권 핵심·대체 핵심지에서 먼저 나타난다.",
        "전국 전면 상승으로 오해하기 쉽지만 실제로는 수도권 선도 회복이 더 강하다.",
        "수도권이 먼저 회복하고 지방은 보조적으로 따라붙는 선별 회복 구간이다.",
    ),
    PhaseDefinition(
        8,
        "phase_08",
        "정책제약 하 선별 확산기",
        202507,
        202603,
        "선별 확산·정책 제약",
        "대출규제·토허 확대·거래관리 강화",
        "정책 제약이 강한 국면에서는 서울 추격보다 대체 핵심지와 임차 수요 기반이 버티는 지역이 먼저 반응한다.",
        "월세화와 공급 부담이 섞인 지역은 투자수요 유입과 실수요 약화를 구분해 읽어야 한다.",
        "핵심지 직접 규제와 대출 제약 아래에서 대체 핵심지 중심 선별 확산이 나타난 현재 구간이다.",
    ),
]


def ym_to_period(ym: int) -> pd.Period:
    return pd.Period(str(int(ym)), freq="M")


def period_to_ym(period: pd.Period) -> int:
    return int(period.strftime("%Y%m"))


def shift_ym(ym: int, months: int) -> int:
    return period_to_ym(ym_to_period(ym) + months)


def format_value(value: object, digits: int = 2) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "-"
    if isinstance(value, (int, np.integer)):
        if 190000 <= int(value) <= 300000:
            return str(int(value))
        return f"{int(value):,}"
    if isinstance(value, (float, np.floating)):
        return f"{float(value):,.{digits}f}"
    return str(value)


def markdown_table(df: pd.DataFrame, digits: int = 2) -> str:
    rows = []
    headers = list(df.columns)
    rows.append("| " + " | ".join(headers) + " |")
    rows.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for _, row in df.iterrows():
        values = [format_value(row[col], digits) for col in headers]
        rows.append("| " + " | ".join(values) + " |")
    return "\n".join(rows)


def region_case(column: str = "시도") -> str:
    return f"""
        CASE
            WHEN {column} IN ('서울특별시', '경기도', '인천광역시') THEN '수도권'
            WHEN {column} IN ('부산광역시', '대구광역시', '광주광역시', '대전광역시', '울산광역시') THEN '지방광역시'
            ELSE '기타지방'
        END
    """


def query_sale_region_monthly(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    query = f"""
        WITH sigungu_base AS (
            SELECT
                계약년월,
                {region_case()} AS 권역,
                시도,
                시군구,
                MEDIAN(매매대표평당가_YoY_pct) AS 시군구_YoY_pct,
                SUM(거래건수) AS 시군구_거래량
            FROM v_sale_monthly_yoy
            WHERE 전용면적_구분 = '중소형'
              AND 계약년월 >= 200701
              AND 매매대표평당가_YoY_pct IS NOT NULL
            GROUP BY 1, 2, 3, 4
        )
        SELECT
            계약년월 AS ym,
            권역,
            MEDIAN(시군구_YoY_pct) AS 평균YoY_pct,
            SUM(시군구_거래량) AS 거래량,
            100.0 * AVG(CASE WHEN 시군구_YoY_pct > 0 THEN 1 ELSE 0 END) AS 상승지역비중_pct,
            COUNT(*) AS 시군구수
        FROM sigungu_base
        GROUP BY 1, 2
        ORDER BY 1, 2
    """
    df = con.execute(query).df()
    df["거래량지수"] = df.groupby("권역")["거래량"].transform(
        lambda s: 100.0 * s / s.median()
    )
    return df


def query_jeonse_region_monthly(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    query = f"""
        WITH sigungu_base AS (
            SELECT
                계약년월,
                {region_case()} AS 권역,
                시도,
                시군구,
                MEDIAN(전세가율_pct) AS 시군구_전세가율_pct
            FROM v_jeonse_ratio_monthly
            WHERE 전용면적_구분 = '중소형'
              AND 계약년월 >= 201101
              AND 전세가율_pct IS NOT NULL
            GROUP BY 1, 2, 3, 4
        )
        SELECT
            계약년월 AS ym,
            권역,
            MEDIAN(시군구_전세가율_pct) AS 전세가율_pct
        FROM sigungu_base
        GROUP BY 1, 2
        ORDER BY 1, 2
    """
    return con.execute(query).df()


def query_wolse_region_monthly(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    query = f"""
        WITH sigungu_base AS (
            SELECT
                계약년월,
                {region_case()} AS 권역,
                시도,
                시군구,
                100.0 * SUM(월세거래건수) / NULLIF(SUM(전체거래건수), 0) AS 시군구_월세비중_pct
            FROM v_lease_conversion_mix_monthly
            WHERE 전용면적_구분 = '중소형'
              AND 계약년월 >= 201101
              AND 전체거래건수 > 0
            GROUP BY 1, 2, 3, 4
        )
        SELECT
            계약년월 AS ym,
            권역,
            MEDIAN(시군구_월세비중_pct) AS 월세비중_pct
        FROM sigungu_base
        GROUP BY 1, 2
        ORDER BY 1, 2
    """
    return con.execute(query).df()


def query_unsold_region_monthly(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    query = f"""
        SELECT
            CAST(REPLACE(시점, '.', '') AS INTEGER) AS ym,
            {region_case()} AS 권역,
            SUM(미분양수) AS 준공후미분양_호수
        FROM KOSIS_준공후미분양
        GROUP BY 1, 2
        ORDER BY 1, 2
    """
    return con.execute(query).df()


def query_movein_18m(con: duckdb.DuckDBPyConnection, latest_sale_ym: int) -> pd.DataFrame:
    start_ym = shift_ym(latest_sale_ym, 1)
    end_ym = shift_ym(latest_sale_ym, 18)
    query = f"""
        SELECT
            권역구분 AS 권역,
            SUM(입주예정세대수) AS 향후18개월_입주예정세대수
        FROM v_movein_plan_region_monthly
        WHERE 입주예정월 BETWEEN {start_ym} AND {end_ym}
        GROUP BY 1
        ORDER BY 1
    """
    df = con.execute(query).df()
    df["시작년월"] = start_ym
    df["종료년월"] = end_ym
    return df


def pivot_metric(df: pd.DataFrame, value_col: str, prefix: str) -> pd.DataFrame:
    wide = df.pivot(index="ym", columns="권역", values=value_col).reset_index()
    wide = wide.rename(
        columns={
            "수도권": f"수도권_{prefix}",
            "지방광역시": f"지방광역시_{prefix}",
            "기타지방": f"기타지방_{prefix}",
        }
    )
    return wide


def prepare_monthly_feature_frame(
    sale_df: pd.DataFrame, jeonse_df: pd.DataFrame, wolse_df: pd.DataFrame
) -> pd.DataFrame:
    frames = [
        pivot_metric(sale_df, "평균YoY_pct", "평균YoY_pct"),
        pivot_metric(sale_df, "상승지역비중_pct", "상승지역비중_pct"),
        pivot_metric(sale_df, "거래량지수", "거래량지수"),
        pivot_metric(jeonse_df, "전세가율_pct", "전세가율_pct"),
        pivot_metric(wolse_df, "월세비중_pct", "월세비중_pct"),
    ]
    merged = frames[0]
    for frame in frames[1:]:
        merged = merged.merge(frame, on="ym", how="inner")
    return merged.sort_values("ym").reset_index(drop=True)


def assign_phase(ym: int) -> PhaseDefinition | None:
    for phase in PHASES:
        if phase.start_ym <= ym <= phase.end_ym:
            return phase
    return None


def classify_lease_stage(
    sudo_jeonse: float, metro_jeonse: float, other_jeonse: float, sudo_wolse: float
) -> str:
    values = [sudo_jeonse, metro_jeonse, other_jeonse]
    if all(pd.isna(v) for v in values) or pd.isna(sudo_wolse):
        return "초기 데이터 구간"
    avg_jeonse = np.nanmean(values)
    if avg_jeonse >= 72 and sudo_wolse < 30:
        return "전세 기반 회복"
    if avg_jeonse >= 63 and sudo_wolse >= 35:
        return "월세화 동반 회복"
    if avg_jeonse < 60 and sudo_wolse >= 35:
        return "월세화 심화"
    return "혼합·전환기"


def classify_supply_stage(
    unsold_values: Iterable[float], region_medians: dict[str, float]
) -> str:
    labels = []
    for region, value in unsold_values:
        if np.isnan(value):
            continue
        median_value = region_medians.get(region)
        if not median_value or np.isnan(median_value):
            continue
        ratio = value / median_value if median_value else np.nan
        if ratio >= 1.2:
            labels.append(f"{region} 부담 높음")
        elif ratio <= 0.8:
            labels.append(f"{region} 부담 낮음")
    if not labels:
        return "초기 데이터 구간"
    return ", ".join(labels[:2])


def build_phase_summary(
    sale_df: pd.DataFrame,
    jeonse_df: pd.DataFrame,
    wolse_df: pd.DataFrame,
    unsold_df: pd.DataFrame,
) -> pd.DataFrame:
    unsold_region_medians = (
        unsold_df.groupby("권역")["준공후미분양_호수"].median().to_dict()
    )
    rows: list[dict[str, object]] = []

    for phase in PHASES:
        sale_slice = sale_df[(sale_df["ym"] >= phase.start_ym) & (sale_df["ym"] <= phase.end_ym)]
        jeonse_slice = jeonse_df[
            (jeonse_df["ym"] >= phase.start_ym) & (jeonse_df["ym"] <= phase.end_ym)
        ]
        wolse_slice = wolse_df[
            (wolse_df["ym"] >= phase.start_ym) & (wolse_df["ym"] <= phase.end_ym)
        ]
        unsold_slice = unsold_df[
            (unsold_df["ym"] >= phase.start_ym) & (unsold_df["ym"] <= phase.end_ym)
        ]

        row: dict[str, object] = {
            "phase_order": phase.order,
            "phase_id": phase.phase_id,
            "국면명": phase.phase_name,
            "시작년월": phase.start_ym,
            "종료년월": phase.end_ym,
            "정책국면태그": phase.policy_tag,
            "매매국면태그": phase.macro_tag,
            "반복패턴요약": phase.repeat_pattern,
            "예외패턴요약": phase.exception_pattern,
            "국면설명": phase.note,
        }

        for region in ["수도권", "지방광역시", "기타지방"]:
            sale_region = sale_slice[sale_slice["권역"] == region]
            jeonse_region = jeonse_slice[jeonse_slice["권역"] == region]
            wolse_region = wolse_slice[wolse_slice["권역"] == region]
            unsold_region = unsold_slice[unsold_slice["권역"] == region]

            row[f"{region}_평균YoY_pct"] = sale_region["평균YoY_pct"].mean()
            row[f"{region}_상승지역비중_pct"] = sale_region["상승지역비중_pct"].mean()
            row[f"{region}_거래량지수"] = sale_region["거래량지수"].mean()
            row[f"{region}_전세가율_pct"] = jeonse_region["전세가율_pct"].mean()
            row[f"{region}_월세비중_pct"] = wolse_region["월세비중_pct"].mean()
            row[f"{region}_준공후미분양_월평균"] = unsold_region["준공후미분양_호수"].mean()

        row["임차국면태그"] = classify_lease_stage(
            row["수도권_전세가율_pct"],
            row["지방광역시_전세가율_pct"],
            row["기타지방_전세가율_pct"],
            row["수도권_월세비중_pct"],
        )
        row["공급국면태그"] = classify_supply_stage(
            [
                ("수도권", row["수도권_준공후미분양_월평균"]),
                ("지방광역시", row["지방광역시_준공후미분양_월평균"]),
                ("기타지방", row["기타지방_준공후미분양_월평균"]),
            ],
            unsold_region_medians,
        )
        rows.append(row)

    return pd.DataFrame(rows)


def build_rolling_similarity(monthly_features: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    rolling = monthly_features.copy()
    feature_cols = [col for col in rolling.columns if col != "ym"]
    rolling[feature_cols] = rolling[feature_cols].rolling(window=6, min_periods=6).mean()
    rolling = rolling.dropna().reset_index(drop=True)

    current_row = rolling.iloc[-1].copy()
    current_end_ym = int(current_row["ym"])
    current_start_ym = shift_ym(current_end_ym, -5)

    historical = rolling[rolling["ym"] < current_start_ym].copy()

    scaler_source = pd.concat([historical[SIMILARITY_FEATURES], current_row.to_frame().T[SIMILARITY_FEATURES]])
    means = scaler_source.mean()
    stds = scaler_source.std(ddof=0).replace(0, 1.0)

    current_vector = (current_row[SIMILARITY_FEATURES] - means) / stds
    historical["distance"] = historical[SIMILARITY_FEATURES].apply(
        lambda row: float(np.sqrt(np.square((row - means) / stds - current_vector).sum())),
        axis=1,
    )
    historical["시작년월"] = historical["ym"].apply(lambda ym: shift_ym(int(ym), -5))
    historical["종료년월"] = historical["ym"].astype(int)
    historical["대표국면"] = historical["종료년월"].apply(
        lambda ym: assign_phase(int(ym)).phase_name if assign_phase(int(ym)) else "미분류"
    )
    historical["정책국면태그"] = historical["종료년월"].apply(
        lambda ym: assign_phase(int(ym)).policy_tag if assign_phase(int(ym)) else "미분류"
    )
    historical["비교단위"] = "6개월 롤링창"
    cols = [
        "비교단위",
        "대표국면",
        "시작년월",
        "종료년월",
        "distance",
        *SIMILARITY_FEATURES,
        "정책국면태그",
    ]
    return historical.sort_values("distance").head(10)[cols].reset_index(drop=True), current_row


def build_phase_similarity(
    phase_summary_df: pd.DataFrame, current_phase_id: str
) -> pd.DataFrame:
    historical = phase_summary_df[
        (phase_summary_df["phase_id"] != current_phase_id)
        & (phase_summary_df["종료년월"] < PHASES[-1].start_ym)
    ].copy()
    historical = historical.dropna(subset=SIMILARITY_FEATURES)
    current = phase_summary_df[phase_summary_df["phase_id"] == current_phase_id].iloc[0]

    scaler_source = pd.concat([historical[SIMILARITY_FEATURES], current.to_frame().T[SIMILARITY_FEATURES]])
    means = scaler_source.mean()
    stds = scaler_source.std(ddof=0).replace(0, 1.0)
    current_vector = (current[SIMILARITY_FEATURES] - means) / stds

    historical["distance"] = historical[SIMILARITY_FEATURES].apply(
        lambda row: float(np.sqrt(np.square((row - means) / stds - current_vector).sum())),
        axis=1,
    )
    historical["비교단위"] = "국면 평균"
    cols = [
        "비교단위",
        "국면명",
        "시작년월",
        "종료년월",
        "distance",
        *SIMILARITY_FEATURES,
        "정책국면태그",
        "매매국면태그",
        "임차국면태그",
    ]
    return historical.sort_values("distance").head(5)[cols].reset_index(drop=True)


def build_current_context(
    phase_summary_df: pd.DataFrame,
    rolling_similarity_df: pd.DataFrame,
    phase_similarity_df: pd.DataFrame,
    movein_df: pd.DataFrame,
    unsold_df: pd.DataFrame,
) -> dict[str, object]:
    current_phase = phase_summary_df.iloc[-1]
    current_end = int(current_phase["종료년월"])
    latest_unsold = (
        unsold_df[unsold_df["ym"] == unsold_df["ym"].max()]
        .pivot(index="ym", columns="권역", values="준공후미분양_호수")
        .reset_index(drop=True)
        .iloc[0]
        .to_dict()
    )

    phase_top = phase_similarity_df.iloc[0]
    rolling_top = rolling_similarity_df.iloc[0]

    return {
        "current_phase": current_phase,
        "phase_top": phase_top,
        "rolling_top": rolling_top,
        "latest_unsold_ym": int(unsold_df["ym"].max()),
        "latest_unsold": latest_unsold,
        "movein": movein_df.set_index("권역")["향후18개월_입주예정세대수"].to_dict(),
        "movein_start": int(movein_df["시작년월"].iloc[0]),
        "movein_end": int(movein_df["종료년월"].iloc[0]),
    }


def write_report(
    phase_summary_df: pd.DataFrame,
    rolling_similarity_df: pd.DataFrame,
    phase_similarity_df: pd.DataFrame,
    context: dict[str, object],
) -> None:
    current_phase = context["current_phase"]
    phase_top = context["phase_top"]
    rolling_top = context["rolling_top"]
    distinct_phase_names = phase_similarity_df["국면명"].tolist()
    secondary_phase_names = ", ".join(distinct_phase_names[1:3]) if len(distinct_phase_names) > 2 else ", ".join(distinct_phase_names[1:])

    phase_table = phase_summary_df[
        [
            "국면명",
            "시작년월",
            "종료년월",
            "매매국면태그",
            "임차국면태그",
            "수도권_평균YoY_pct",
            "지방광역시_평균YoY_pct",
            "기타지방_평균YoY_pct",
            "수도권_전세가율_pct",
            "수도권_월세비중_pct",
        ]
    ].copy()
    phase_similarity_table = phase_similarity_df[
        ["국면명", "시작년월", "종료년월", "distance", "매매국면태그", "임차국면태그"]
    ].copy()
    rolling_similarity_table = rolling_similarity_df[
        ["대표국면", "시작년월", "종료년월", "distance", "정책국면태그"]
    ].copy()

    movein = context["movein"]
    latest_unsold = context["latest_unsold"]

    report = f"""# 장기 패턴 라이브러리 및 유사국면 정식화 (2026-03-14, codex)

## 1. 목적

- 이 문서는 `3. 매매`, `4. 임차`, `5. 정책`에서 흩어져 있던 장기 역사와 유사국면 비교를 `공통 라이브러리`로 묶는 단계다.
- 목표는 `장기 국면 요약 -> 현재 국면 위치 -> 과거 유사 국면 비교 -> 이번 국면만의 차이`를 한 번에 읽을 수 있게 만드는 것이다.
- 이번 버전은 새 규칙을 임의로 추가한 것이 아니라, 이미 확정된 매매·임차·정책 심화 보고서의 공통 축을 외장 DuckDB 기준으로 다시 정리한 것이다.

## 2. 데이터와 정식화 방식

- 매매 장기축: `v_sale_monthly_yoy`, `2006-01 ~ 2026-03`
- 임차 장기축: `v_jeonse_ratio_monthly`, `v_lease_conversion_mix_monthly`, `2011-01 ~ 2026-03`
- 공급 보조축: `KOSIS_준공후미분양`, `2010-01 ~ 2026-01`
- 향후 입주예정 보조축: `v_movein_plan_region_monthly`, `2026-01 ~ 2027-12`

이번 정식화의 기본 단위는 `권역 x 월`이다.

- 권역은 `수도권`, `지방광역시`, `기타지방` 3개로 고정했다.
- 매매는 시군구 중위 YoY, 상승지역비중, 거래량지수로 요약했다.
- 임차는 전세가율과 월세비중으로 요약했다.
- 현재-과거 유사도는 최근 `6개월 롤링 평균`을 사용했고, 현재 창과 겹치는 직전 구간은 제외했다.
- 현재 국면 서술은 `2025-07 ~ 2026-03`의 정책제약 하 선별 확산기로 두고, 유사도 계산용 현재 창은 최신 6개월 `2025-10 ~ 2026-03`이다.

## 3. 장기 국면 라이브러리 요약

{markdown_table(phase_table, digits=2)}

### 3.1 반복 패턴

- `핵심지 선행`: 침체기에도 서울 핵심과 대체 핵심지는 기사·거래에서 먼저 움직였다.
- `전세 선행`: 회복 초기에는 매매보다 전세가율과 분양시장 반응이 먼저 강해졌다.
- `정책은 트리거`: 정책은 방향을 바꾸는 계기일 수 있지만, 전세 부담·공급·거래 회복이 같이 붙을 때만 지속력이 생겼다.
- `말기 확산 주의`: 전국 동조화 급등기에는 외곽과 저유동성 자산까지 확산되며 과열 신호가 넓어졌다.
- `스팟보다 플로우`: 강남 일부 반등, 단일 월 급등락, 기사 한 건보다 최근 6~24개월 흐름이 더 설명력이 컸다.

### 3.2 예외 패턴

- 조정기에도 핵심지는 가격보다 거래 둔화로 먼저 반응해 `가격만 보면 덜 약해 보이는 착시`가 생긴다.
- 비토허 지역의 월세화는 `실수요 약화`로 단정할 수 없고, 투자수요 유입 가능성과 함께 읽어야 한다.
- 현재는 `초저금리 급등기`처럼 전국 동조화가 아니라, `정책 제약 아래 수도권 선별 확산`에 더 가깝다.

## 4. 현재 국면 진단

### 4.1 사실

- 현재 국면은 `{current_phase['국면명']}`이며, 수도권 평균 YoY는 `{format_value(current_phase['수도권_평균YoY_pct'])}%`, 지방광역시는 `{format_value(current_phase['지방광역시_평균YoY_pct'])}%`, 기타지방은 `{format_value(current_phase['기타지방_평균YoY_pct'])}%`다.
- 수도권 상승지역비중은 `{format_value(current_phase['수도권_상승지역비중_pct'])}%`로, 지방광역시 `{format_value(current_phase['지방광역시_상승지역비중_pct'])}%`, 기타지방 `{format_value(current_phase['기타지방_상승지역비중_pct'])}%`보다 높다.
- 수도권 전세가율은 `{format_value(current_phase['수도권_전세가율_pct'])}%`, 수도권 월세비중은 `{format_value(current_phase['수도권_월세비중_pct'])}%`다.
- `2026-01` 기준 준공후 미분양은 수도권 `{format_value(latest_unsold.get('수도권'), 0)}호`, 지방광역시 `{format_value(latest_unsold.get('지방광역시'), 0)}호`, 기타지방 `{format_value(latest_unsold.get('기타지방'), 0)}호`다.
- 향후 18개월 `{context['movein_start']} ~ {context['movein_end']}` 입주예정 세대수는 수도권 `{format_value(movein.get('수도권'), 0)}세대`, 지방광역시 `{format_value(movein.get('지방광역시'), 0)}세대`, 기타지방 `{format_value(movein.get('기타지방'), 0)}세대`다.

### 4.2 해석

- 현재는 `전국적 대세 상승 초입`보다 `정책제약 아래 수도권과 대체 핵심지 중심 선별 확산`으로 읽는 편이 맞다.
- 수도권 전세가율 회복과 월세화 누적이 동시에 보여, 임차 수요는 살아 있지만 계약 구조는 과거 회복기보다 더 거칠다.
- 지방광역시와 기타지방은 일부 선별 회복이 있지만, 준공후 미분양과 출구 리스크 때문에 전국 동조화로 읽기는 어렵다.

### 4.3 가설

- 향후 6~12개월은 `서울 핵심 추격`보다 `정책 제약을 덜 받는 대체 핵심지`, `임차 수요 기반이 유지되는 곳`, `준공후 미분양 압력이 낮은 곳`이 먼저 반응할 가능성이 높다.
- 반대로 외곽과 지방의 약한 생활권은 단기 반등이 나와도 `현재 국면의 본류`라기보다 국지적 반응일 가능성이 높다.

## 5. 현재와 닮은 과거 국면

### 5.1 국면 평균 기준 유사도

{markdown_table(phase_similarity_table, digits=3)}

- 현재 국면 평균과 가장 가까운 장기 비교군은 `{phase_top['국면명']}`이다.
- 그 다음 비교군은 `{secondary_phase_names}` 계열이며, 공통점은 `수도권 선도`, `임차 회복`, `전국 비동조화`다.
- 차이점은 이번 구간이 과거보다 `월세화 누적`과 `정책 제약`, `지방 공급 부담`이 더 크다는 점이다.

### 5.2 6개월 롤링창 기준 유사도

{markdown_table(rolling_similarity_table, digits=3)}

- 가장 가까운 실제 구간은 `{rolling_top['시작년월']}~{rolling_top['종료년월']}`이며, 대표 국면은 `{rolling_top['대표국면']}`이다.
- 최근 유사창이 주로 `2024년 회복기`에 몰린다는 것은, 현재가 `새로운 전국 급등`보다 `2024년 수도권 선도 회복의 연장선`에 더 가깝다는 뜻이다.
- 다만 이번 구간은 `2025-07 대출규제`, `2025-03 토허 확대`, `거래관리 강화`가 겹쳐 있어 과거 회복기보다 정책 필터가 훨씬 두껍다.

## 6. 이번 국면만의 차이

- `정책 제약`: 과거 회복기보다 대출·토허·거래관리 규제가 강하다.
- `월세화`: 전세가율은 회복됐지만 수도권 월세비중이 과거 회복기보다 높다.
- `지방 공급 부담`: 지방광역시와 기타지방은 가격이 조금 반등해도 준공후 미분양 부담이 여전히 크다.
- `선별성`: 과거 전국 동조화기와 달리, 지금은 지역 안에서도 생활권과 상품성에 따라 강약이 더 크게 갈린다.

## 7. 실전 해석 원칙

- 서울 핵심지 일부 반등을 `전국 상승장 신호`로 번역하지 않는다.
- 전세가율 회복은 중요한 신호지만, 월세화 동반 여부와 함께 읽는다.
- 정책은 단독 원인이 아니라 `전세·거래·공급 구조가 받쳐줄 때만` 지속력이 생긴다고 본다.
- 말기 확산처럼 외곽과 저유동성 자산으로 급격히 번질 때는 오히려 리스크 플래그를 높인다.

## 8. 관련 산출물

- 국면 요약표: [00_장기패턴_국면요약_20260314_codex.csv](./00_%EC%9E%A5%EA%B8%B0%ED%8C%A8%ED%84%B4_%EA%B5%AD%EB%A9%B4%EC%9A%94%EC%95%BD_20260314_codex.csv)
- 유사국면 비교표: [00_현재국면_과거유사국면비교_20260314_codex.csv](./00_%ED%98%84%EC%9E%AC%EA%B5%AD%EB%A9%B4_%EA%B3%BC%EA%B1%B0%EC%9C%A0%EC%82%AC%EA%B5%AD%EB%A9%B4%EB%B9%84%EA%B5%90_20260314_codex.csv)
- 매매 심화: [01_매매시장분석_20260314_codex_2차심화.md](../01_%EB%A7%A4%EB%A7%A4%EC%8B%9C%EC%9E%A5/01_%EB%A7%A4%EB%A7%A4%EC%8B%9C%EC%9E%A5%EB%B6%84%EC%84%9D_20260314_codex_2%EC%B0%A8%EC%8B%AC%ED%99%94.md)
- 임차 심화: [02_임차시장분석_20260314_codex_2차심화.md](../02_%EC%9E%84%EC%B0%A8%EC%8B%9C%EC%9E%A5/02_%EC%9E%84%EC%B0%A8%EC%8B%9C%EC%9E%A5%EB%B6%84%EC%84%9D_20260314_codex_2%EC%B0%A8%EC%8B%AC%ED%99%94.md)
- 정책 보강: [03_정책영향분석_20260314_codex_2차보강.md](../03_%EC%A0%95%EC%B1%85%EC%98%81%ED%96%A5/03_%EC%A0%95%EC%B1%85%EC%98%81%ED%96%A5%EB%B6%84%EC%84%9D_20260314_codex_2%EC%B0%A8%EB%B3%B4%EA%B0%95.md)

## 9. 한계

- 유사도 계산은 `권역 3개 x 6개월 롤링 평균` 수준이라, 개별 생활권과 단지 상품성 차이는 직접 반영하지 않는다.
- 입주예정 물량은 미래 데이터라 장기 유사도 거리 계산에는 넣지 않고, 현재 국면 설명용으로만 썼다.
- 준공후 미분양은 `2026-01`까지라 매매·임차 최신월보다 2개월 느리다.
- 2006~2010 구간은 임차 구조 데이터가 없어 매매 축 위주로 해석해야 한다.
"""

    REPORT_PATH.write_text(report, encoding="utf-8")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(DB_PATH, read_only=True)

    sale_df = query_sale_region_monthly(con)
    jeonse_df = query_jeonse_region_monthly(con)
    wolse_df = query_wolse_region_monthly(con)
    unsold_df = query_unsold_region_monthly(con)

    latest_sale_ym = int(sale_df["ym"].max())
    movein_df = query_movein_18m(con, latest_sale_ym)

    phase_summary_df = build_phase_summary(sale_df, jeonse_df, wolse_df, unsold_df)
    monthly_features = prepare_monthly_feature_frame(sale_df, jeonse_df, wolse_df)
    rolling_similarity_df, _ = build_rolling_similarity(monthly_features)
    phase_similarity_df = build_phase_similarity(phase_summary_df, "phase_08")

    phase_summary_df.to_csv(PHASE_SUMMARY_PATH, index=False, encoding="utf-8-sig")
    rolling_similarity_df.to_csv(SIMILARITY_PATH, index=False, encoding="utf-8-sig")

    context = build_current_context(
        phase_summary_df, rolling_similarity_df, phase_similarity_df, movein_df, unsold_df
    )
    write_report(phase_summary_df, rolling_similarity_df, phase_similarity_df, context)

    print(f"generated: {PHASE_SUMMARY_PATH}")
    print(f"generated: {SIMILARITY_PATH}")
    print(f"generated: {REPORT_PATH}")


if __name__ == "__main__":
    main()
