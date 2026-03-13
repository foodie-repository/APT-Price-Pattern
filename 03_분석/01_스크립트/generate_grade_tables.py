from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd


DB_PATH = Path("/Volumes/T9/duckdb-analytics/db/apartment.duckdb")
OUTPUT_DIR = Path("02_데이터/02_참조")

CAPITAL_REGION = ("서울특별시", "경기도", "인천광역시")
LOCAL_METRO_REGION = ("부산광역시", "대구광역시", "광주광역시", "대전광역시", "울산광역시")
AREA_BUCKET = "중소형"


@dataclass(frozen=True)
class RankSpec:
    output_name: str
    region_label: str
    rank_label: str
    level_label: str
    comparison_group: str
    target_bins: int
    min_trades: int
    trade_scope: str
    partition_cols: tuple[str, ...]


CAPITAL_SIGUNGU_SPEC = RankSpec(
    output_name="수도권_매매_급지표_시군구",
    region_label="수도권",
    rank_label="10급지",
    level_label="시군구",
    comparison_group="수도권 전체",
    target_bins=10,
    min_trades=50,
    trade_scope="수도권 전체 비교",
    partition_cols=(),
)

CAPITAL_EUP_SPEC = RankSpec(
    output_name="수도권_매매_급지표_읍면동",
    region_label="수도권",
    rank_label="10급지",
    level_label="읍면동",
    comparison_group="수도권 전체",
    target_bins=10,
    min_trades=50,
    trade_scope="수도권 전체 비교",
    partition_cols=(),
)

LOCAL_METRO_SIGUNGU_SPEC = RankSpec(
    output_name="지방광역시_매매_상대서열표_시군구",
    region_label="지방광역시",
    rank_label="5단계 상대서열",
    level_label="시군구",
    comparison_group="광역시 내부",
    target_bins=5,
    min_trades=30,
    trade_scope="광역시 내부 비교",
    partition_cols=("시도",),
)

LOCAL_METRO_EUP_SPEC = RankSpec(
    output_name="지방광역시_매매_상대서열표_읍면동",
    region_label="지방광역시",
    rank_label="5단계 상대서열",
    level_label="읍면동",
    comparison_group="광역시 내부",
    target_bins=5,
    min_trades=20,
    trade_scope="광역시 내부 비교",
    partition_cols=("시도",),
)

MAJOR_LOCAL_CITY_SIGUNGU_SPEC = RankSpec(
    output_name="주요중소도시_매매_상대서열표_시군구",
    region_label="주요중소도시",
    rank_label="3단계 상대서열",
    level_label="시군구",
    comparison_group="도시 내부",
    target_bins=3,
    min_trades=20,
    trade_scope="도시 내부 비교",
    partition_cols=("시도", "도시루트"),
)

MAJOR_LOCAL_CITY_EUP_SPEC = RankSpec(
    output_name="주요중소도시_매매_상대서열표_읍면동",
    region_label="주요중소도시",
    rank_label="3단계 상대서열",
    level_label="읍면동",
    comparison_group="도시 내부",
    target_bins=3,
    min_trades=15,
    trade_scope="도시 내부 비교",
    partition_cols=("시도", "도시루트"),
)


def _latest_full_year(con: duckdb.DuckDBPyConnection) -> int:
    latest_ym = con.sql("select max(계약년월) from v_sale_clean").fetchone()[0]
    latest_year = latest_ym // 100
    latest_month = latest_ym % 100
    return latest_year if latest_month == 12 else latest_year - 1


def _period(con: duckdb.DuckDBPyConnection) -> tuple[int, int, str]:
    end_year = _latest_full_year(con)
    start_year = end_year - 2
    start_ym = start_year * 100 + 1
    end_ym = end_year * 100 + 12
    return start_ym, end_ym, f"{start_ym}~{end_ym}"


def _sql_values(values: tuple[str, ...]) -> str:
    return "(" + ", ".join(f"'{value}'" for value in values) + ")"


def _capital_group(grade: int) -> str:
    if grade <= 3:
        return "상급지"
    if grade <= 6:
        return "중급지"
    return "하급지"


def _relative_group(grade: int, bins: int) -> str:
    if bins <= 1:
        return "단일권역"
    if bins == 2:
        return "상위권" if grade == 1 else "하위권"
    upper_cut = max(1, (bins + 2) // 3)
    lower_cut = max(upper_cut + 1, (2 * bins + 2) // 3)
    if grade <= upper_cut:
        return "상위권"
    if grade <= lower_cut:
        return "중위권"
    return "하위권"


def _assign_ranks(df: pd.DataFrame, spec: RankSpec, score_kind: str) -> pd.DataFrame:
    if df.empty:
        return df

    sort_cols = list(spec.partition_cols) + ["평단가_중앙값", "거래건수"]
    ascending = [True] * len(spec.partition_cols) + [False, False]
    df = df.sort_values(sort_cols, ascending=ascending).copy()

    ranked_frames: list[pd.DataFrame] = []
    partitions = [()] if not spec.partition_cols else df.groupby(list(spec.partition_cols), sort=False, dropna=False)

    if not spec.partition_cols:
        partitions = [((), df)]

    for _, part in partitions:
        part = part.reset_index(drop=True).copy()
        bins = min(spec.target_bins, len(part))
        part["서열단계수"] = bins
        part["급지"] = (part.index * bins // len(part)) + 1
        part["급지_점수"] = bins + 1 - part["급지"]
        if score_kind == "capital":
            part["급지그룹"] = part["급지"].map(_capital_group)
        else:
            part["급지그룹"] = part["급지"].map(lambda grade: _relative_group(int(grade), bins))
        ranked_frames.append(part)

    return pd.concat(ranked_frames, ignore_index=True)


def _with_metadata(
    df: pd.DataFrame,
    spec: RankSpec,
    period_label: str,
    method_note: str,
    include_city_root: bool = False,
) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df.insert(0, "분석권역", spec.region_label)
    df.insert(1, "서열체계", spec.rank_label)
    df.insert(2, "급지레벨", spec.level_label)
    df.insert(3, "비교집단", spec.comparison_group)
    df.insert(4, "기준기간", period_label)
    df.insert(5, "전용면적_구분", AREA_BUCKET)
    df.insert(6, "최소거래건수", spec.min_trades)
    df["비고"] = ""
    df["산정방식"] = method_note
    df["데이터출처"] = "apartment.duckdb/v_sale_clean"

    base_columns = [
        "분석권역",
        "서열체계",
        "급지레벨",
        "비교집단",
        "기준기간",
        "전용면적_구분",
        "최소거래건수",
        "시도",
    ]
    if include_city_root:
        base_columns.append("도시루트")
    base_columns.extend(["시군구"])
    if "읍면동" in df.columns:
        base_columns.append("읍면동")
    base_columns.extend(
        [
            "평단가_평균",
            "평단가_중앙값",
            "거래건수",
            "서열단계수",
            "급지",
            "급지_점수",
            "급지그룹",
            "비고",
            "산정방식",
            "데이터출처",
        ]
    )
    return df[base_columns]


def _load_capital_sigungu(con: duckdb.DuckDBPyConnection, start_ym: int, end_ym: int) -> pd.DataFrame:
    query = f"""
    with parsed as (
        select
            시도,
            시군구_분리 as 시군구,
            거래금액_만원 / nullif(전용면적_㎡ * 0.3025, 0) as 평단가
        from v_sale_clean
        where 계약년월 between {start_ym} and {end_ym}
          and 시도 in {_sql_values(CAPITAL_REGION)}
          and 전용면적_구분 = '{AREA_BUCKET}'
          and coalesce(trim(시군구_분리), '') <> ''
    )
    select
        시도,
        시군구,
        avg(평단가) as 평단가_평균,
        median(평단가) as 평단가_중앙값,
        count(*) as 거래건수
    from parsed
    group by 1, 2
    having count(*) >= {CAPITAL_SIGUNGU_SPEC.min_trades}
    """
    return con.sql(query).df()


def _load_capital_eupmyeondong(con: duckdb.DuckDBPyConnection, start_ym: int, end_ym: int) -> pd.DataFrame:
    query = f"""
    with parsed as (
        select
            시도,
            시군구_분리 as 시군구,
            읍면동,
            거래금액_만원 / nullif(전용면적_㎡ * 0.3025, 0) as 평단가
        from v_sale_clean
        where 계약년월 between {start_ym} and {end_ym}
          and 시도 in {_sql_values(CAPITAL_REGION)}
          and 전용면적_구분 = '{AREA_BUCKET}'
          and coalesce(trim(시군구_분리), '') <> ''
          and coalesce(trim(읍면동), '') <> ''
    )
    select
        시도,
        시군구,
        읍면동,
        avg(평단가) as 평단가_평균,
        median(평단가) as 평단가_중앙값,
        count(*) as 거래건수
    from parsed
    group by 1, 2, 3
    having count(*) >= {CAPITAL_EUP_SPEC.min_trades}
    """
    return con.sql(query).df()


def _load_local_metro_sigungu(con: duckdb.DuckDBPyConnection, start_ym: int, end_ym: int) -> pd.DataFrame:
    query = f"""
    with parsed as (
        select
            시도,
            시군구_분리 as 시군구,
            거래금액_만원 / nullif(전용면적_㎡ * 0.3025, 0) as 평단가
        from v_sale_clean
        where 계약년월 between {start_ym} and {end_ym}
          and 시도 in {_sql_values(LOCAL_METRO_REGION)}
          and 전용면적_구분 = '{AREA_BUCKET}'
          and coalesce(trim(시군구_분리), '') <> ''
    )
    select
        시도,
        시군구,
        avg(평단가) as 평단가_평균,
        median(평단가) as 평단가_중앙값,
        count(*) as 거래건수
    from parsed
    group by 1, 2
    having count(*) >= {LOCAL_METRO_SIGUNGU_SPEC.min_trades}
    """
    return con.sql(query).df()


def _load_local_metro_eupmyeondong(con: duckdb.DuckDBPyConnection, start_ym: int, end_ym: int) -> pd.DataFrame:
    query = f"""
    with parsed as (
        select
            시도,
            시군구_분리 as 시군구,
            읍면동,
            거래금액_만원 / nullif(전용면적_㎡ * 0.3025, 0) as 평단가
        from v_sale_clean
        where 계약년월 between {start_ym} and {end_ym}
          and 시도 in {_sql_values(LOCAL_METRO_REGION)}
          and 전용면적_구분 = '{AREA_BUCKET}'
          and coalesce(trim(시군구_분리), '') <> ''
          and coalesce(trim(읍면동), '') <> ''
    )
    select
        시도,
        시군구,
        읍면동,
        avg(평단가) as 평단가_평균,
        median(평단가) as 평단가_중앙값,
        count(*) as 거래건수
    from parsed
    group by 1, 2, 3
    having count(*) >= {LOCAL_METRO_EUP_SPEC.min_trades}
    """
    return con.sql(query).df()


def _load_major_local_city_roots(con: duckdb.DuckDBPyConnection, start_ym: int, end_ym: int) -> pd.DataFrame:
    excluded_regions = CAPITAL_REGION + LOCAL_METRO_REGION
    query = f"""
    with base as (
        select
            시도,
            case
                when strpos(시군구_분리, ' ') > 0 then split_part(시군구_분리, ' ', 1)
                else 시군구_분리
            end as 도시루트,
            count(*) as 거래건수
        from v_sale_clean
        where 계약년월 between {start_ym} and {end_ym}
          and 전용면적_구분 = '{AREA_BUCKET}'
          and 시도 not in {_sql_values(excluded_regions)}
          and coalesce(trim(시군구_분리), '') <> ''
        group by 1, 2
    )
    select
        시도,
        도시루트,
        거래건수
    from base
    where 거래건수 >= 2500
      and 도시루트 like '%시'
    order by 시도, 도시루트
    """
    return con.sql(query).df()


def _load_major_local_city_sigungu(
    con: duckdb.DuckDBPyConnection,
    start_ym: int,
    end_ym: int,
    major_city_roots: pd.DataFrame,
) -> pd.DataFrame:
    if major_city_roots.empty:
        return pd.DataFrame()

    city_pairs = ", ".join(
        f"('{row.시도}', '{row.도시루트}')" for row in major_city_roots.itertuples(index=False)
    )
    query = f"""
    with parsed as (
        select
            시도,
            case
                when strpos(시군구_분리, ' ') > 0 then split_part(시군구_분리, ' ', 1)
                else 시군구_분리
            end as 도시루트,
            시군구_분리 as 시군구,
            거래금액_만원 / nullif(전용면적_㎡ * 0.3025, 0) as 평단가
        from v_sale_clean
        where 계약년월 between {start_ym} and {end_ym}
          and 전용면적_구분 = '{AREA_BUCKET}'
          and coalesce(trim(시군구_분리), '') <> ''
    ),
    filtered as (
        select *
        from parsed
        where (시도, 도시루트) in ({city_pairs})
    )
    select
        시도,
        도시루트,
        시군구,
        avg(평단가) as 평단가_평균,
        median(평단가) as 평단가_중앙값,
        count(*) as 거래건수
    from filtered
    group by 1, 2, 3
    having count(*) >= {MAJOR_LOCAL_CITY_SIGUNGU_SPEC.min_trades}
    """
    return con.sql(query).df()


def _load_major_local_city_eupmyeondong(
    con: duckdb.DuckDBPyConnection,
    start_ym: int,
    end_ym: int,
    major_city_roots: pd.DataFrame,
) -> pd.DataFrame:
    if major_city_roots.empty:
        return pd.DataFrame()

    city_pairs = ", ".join(
        f"('{row.시도}', '{row.도시루트}')" for row in major_city_roots.itertuples(index=False)
    )
    query = f"""
    with parsed as (
        select
            시도,
            case
                when strpos(시군구_분리, ' ') > 0 then split_part(시군구_분리, ' ', 1)
                else 시군구_분리
            end as 도시루트,
            시군구_분리 as 시군구,
            읍면동,
            거래금액_만원 / nullif(전용면적_㎡ * 0.3025, 0) as 평단가
        from v_sale_clean
        where 계약년월 between {start_ym} and {end_ym}
          and 전용면적_구분 = '{AREA_BUCKET}'
          and coalesce(trim(시군구_분리), '') <> ''
          and coalesce(trim(읍면동), '') <> ''
    ),
    filtered as (
        select *
        from parsed
        where (시도, 도시루트) in ({city_pairs})
    )
    select
        시도,
        도시루트,
        시군구,
        읍면동,
        avg(평단가) as 평단가_평균,
        median(평단가) as 평단가_중앙값,
        count(*) as 거래건수
    from filtered
    group by 1, 2, 3, 4
    having count(*) >= {MAJOR_LOCAL_CITY_EUP_SPEC.min_trades}
    """
    return con.sql(query).df()


def _save_frame(frame: pd.DataFrame, output_name: str, today: str) -> Path | None:
    if frame.empty:
        return None
    path = OUTPUT_DIR / f"{output_name}_{today}.csv"
    frame.to_csv(path, index=False, encoding="utf-8-sig")
    return path


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    today = date.today().strftime("%Y%m%d")

    con = duckdb.connect(str(DB_PATH), read_only=True)
    start_ym, end_ym, period_label = _period(con)

    capital_sigungu = _with_metadata(
        _assign_ranks(_load_capital_sigungu(con, start_ym, end_ym), CAPITAL_SIGUNGU_SPEC, "capital"),
        CAPITAL_SIGUNGU_SPEC,
        period_label,
        "현재 DuckDB 재산출(수도권 10급지)",
    )
    capital_eup = _with_metadata(
        _assign_ranks(_load_capital_eupmyeondong(con, start_ym, end_ym), CAPITAL_EUP_SPEC, "capital"),
        CAPITAL_EUP_SPEC,
        period_label,
        "현재 DuckDB 재산출(수도권 10급지)",
    )
    local_metro_sigungu = _with_metadata(
        _assign_ranks(_load_local_metro_sigungu(con, start_ym, end_ym), LOCAL_METRO_SIGUNGU_SPEC, "relative"),
        LOCAL_METRO_SIGUNGU_SPEC,
        period_label,
        "현재 DuckDB 재산출(광역시 내부 5단계 상대서열)",
    )
    local_metro_eup = _with_metadata(
        _assign_ranks(_load_local_metro_eupmyeondong(con, start_ym, end_ym), LOCAL_METRO_EUP_SPEC, "relative"),
        LOCAL_METRO_EUP_SPEC,
        period_label,
        "현재 DuckDB 재산출(광역시 내부 5단계 상대서열)",
    )

    major_city_roots = _load_major_local_city_roots(con, start_ym, end_ym)
    major_local_city_sigungu = _with_metadata(
        _assign_ranks(
            _load_major_local_city_sigungu(con, start_ym, end_ym, major_city_roots),
            MAJOR_LOCAL_CITY_SIGUNGU_SPEC,
            "relative",
        ),
        MAJOR_LOCAL_CITY_SIGUNGU_SPEC,
        period_label,
        "현재 DuckDB 재산출(도시 내부 3단계 상대서열)",
        include_city_root=True,
    )
    major_local_city_eup = _with_metadata(
        _assign_ranks(
            _load_major_local_city_eupmyeondong(con, start_ym, end_ym, major_city_roots),
            MAJOR_LOCAL_CITY_EUP_SPEC,
            "relative",
        ),
        MAJOR_LOCAL_CITY_EUP_SPEC,
        period_label,
        "현재 DuckDB 재산출(도시 내부 3단계 상대서열)",
        include_city_root=True,
    )
    con.close()

    outputs = {
        CAPITAL_SIGUNGU_SPEC.output_name: capital_sigungu,
        CAPITAL_EUP_SPEC.output_name: capital_eup,
        LOCAL_METRO_SIGUNGU_SPEC.output_name: local_metro_sigungu,
        LOCAL_METRO_EUP_SPEC.output_name: local_metro_eup,
        MAJOR_LOCAL_CITY_SIGUNGU_SPEC.output_name: major_local_city_sigungu,
        MAJOR_LOCAL_CITY_EUP_SPEC.output_name: major_local_city_eup,
    }

    for output_name, frame in outputs.items():
        path = _save_frame(frame, output_name, today)
        if path is None:
            print(f"{output_name}: no rows")
        else:
            print(path)
            print(f"rows={len(frame)}")


if __name__ == "__main__":
    main()
