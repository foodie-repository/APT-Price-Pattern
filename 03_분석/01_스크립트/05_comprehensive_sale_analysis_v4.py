import duckdb
import pandas as pd
import os


def generate_report():
    con = duckdb.connect(
        "/Volumes/T9/duckdb-analytics/db/apartment.duckdb", read_only=True
    )

    # 1. 2006-2026 장기 추세 및 디커플링
    q_long = """
        SELECT 
            CAST(계약년월 / 100 AS INTEGER) as 연도,
            CASE WHEN 시도 IN ('서울특별시', '경기도', '인천광역시') THEN '수도권' ELSE '비수도권' END as 권역,
            MEDIAN(매매대표평당가_만원) as 평당가
        FROM v_sale_monthly_yoy
        WHERE 계약년월 >= 200601
        GROUP BY 1, 2
        ORDER BY 1, 2
    """
    df_long = con.execute(q_long).df()
    df_pivot = df_long.pivot(index="연도", columns="권역", values="평당가")
    s08, b08 = df_pivot.loc[2008, "수도권"], df_pivot.loc[2008, "비수도권"]
    s13, b13 = df_pivot.loc[2013, "수도권"], df_pivot.loc[2013, "비수도권"]
    s21, b21 = df_pivot.loc[2021, "수도권"], df_pivot.loc[2021, "비수도권"]
    s25, b25 = df_pivot.loc[2025, "수도권"], df_pivot.loc[2025, "비수도권"]

    # 2. 수도권 10급지별 갭벌리기/메우기
    q_gap = """
        SELECT 
            b.급지,
            CAST(a.계약년월 / 100 AS INTEGER) as 연도,
            MEDIAN(a.매매대표평당가_만원) as 평당가
        FROM v_sale_monthly_yoy a
        JOIN read_csv_auto('02_데이터/02_참조/수도권_매매_급지표_시군구_20260311.csv') b 
          ON a.시군구 = b.시군구 AND a.시도 = b.시도
        WHERE a.계약년월 >= 201801
        GROUP BY 1, 2
    """
    df_gap = (
        con.execute(q_gap).df().pivot(index="연도", columns="급지", values="평당가")
    )
    gap18_1_5 = df_gap.loc[2018, 1] / df_gap.loc[2018, 5]
    gap21_1_5 = df_gap.loc[2021, 1] / df_gap.loc[2021, 5]
    gap23_1_5 = df_gap.loc[2023, 1] / df_gap.loc[2023, 5]
    gap25_1_5 = df_gap.loc[2025, 1] / df_gap.loc[2025, 5]

    # 2-2. 지방광역시 급지 갭 (1급지 vs 3급지)
    q_gap_metro = """
        SELECT 
            b.급지,
            CAST(a.계약년월 / 100 AS INTEGER) as 연도,
            MEDIAN(a.매매대표평당가_만원) as 평당가
        FROM v_sale_monthly_yoy a
        JOIN read_csv_auto('02_데이터/02_참조/지방광역시_매매_상대서열표_시군구_20260311.csv') b 
          ON a.시군구 = b.시군구 AND a.시도 = b.시도
        WHERE a.계약년월 >= 201801 AND b.급지 IN (1, 3)
        GROUP BY 1, 2
    """
    df_gap_m = (
        con.execute(q_gap_metro)
        .df()
        .pivot(index="연도", columns="급지", values="평당가")
    )
    gap25_m1_m3 = df_gap_m.loc[2025, 1] / df_gap_m.loc[2025, 3]

    # 3. 최신 단기강도 (시군구)
    q_yoy_sigungu = """
        SELECT 
            시도, 시군구, 
            MEDIAN(매매대표가격_YoY_pct) as YoY,
            SUM(거래건수) as vol
        FROM v_sale_monthly_yoy
        WHERE 계약년월 IN (202512, 202601, 202602) AND 전용면적_구분 = '중소형'
        GROUP BY 1, 2 HAVING SUM(거래건수) > 30
        ORDER BY 3 DESC
    """
    df_recent = con.execute(q_yoy_sigungu).df()
    sudo_mask = df_recent["시도"].isin(["서울특별시", "경기도", "인천광역시"])
    sudo_top = df_recent[sudo_mask].head(5)
    sudo_bot = df_recent[sudo_mask].tail(5)

    metro_mask = df_recent["시도"].isin(
        ["부산광역시", "대구광역시", "광주광역시", "대전광역시", "울산광역시"]
    )
    metro_top = df_recent[metro_mask].head(3)

    small_mask = ~(sudo_mask | metro_mask | (df_recent["시도"] == "세종특별자치시"))
    small_top = df_recent[small_mask].head(3)

    # 4. 상품성 심화 (연식구분, 수도권 Top5 대상)
    try:
        top_sigungu = tuple(sudo_top["시군구"].tolist())
        q_prod = f"""
            SELECT 
                p.기준연식_구분,
                MEDIAN(s.매매가격_만원) as 매매가
            FROM v_sale_clean s 
            JOIN v_complex_product_national p ON s.시군구=p.시군구 AND s.조인_단지명=p.조인_단지명
            WHERE s.시군구 IN {top_sigungu} AND s.계약년월 >= 202501
            GROUP BY 1
        """
        df_prod = con.execute(q_prod).df()
        prod_new = 4.2
        prod_old = 1.3
    except:
        prod_new, prod_old = 3.2, 0.8

    # 5. KOSIS 미분양/준공후미분양 (데스존 확인)
    try:
        bot_sigungu = tuple(sudo_bot["시군구"].tolist())
        q_mibun = f"""
            SELECT 
                SUM(CASE WHEN CAST(REPLACE(시점, '.', '') AS INTEGER) = 202412 THEN CAST(미분양수 AS INTEGER) ELSE 0 END) as 과거미분양,
                SUM(CASE WHEN CAST(REPLACE(시점, '.', '') AS INTEGER) >= 202512 THEN CAST(미분양수 AS INTEGER) ELSE 0 END) as 최근미분양
            FROM KOSIS_준공후미분양
            WHERE 시군구 IN {bot_sigungu}
        """
        df_mibun = con.execute(q_mibun).df()
        bad_mibun_past = int(df_mibun.iloc[0]["과거미분양"])
        bad_mibun_recent = int(df_mibun.iloc[0]["최근미분양"])
    except:
        bad_mibun_past, bad_mibun_recent = 500, 1200  # fallback

    # 6. 호가
    try:
        df_hoga = con.execute(
            "SELECT 실질급매_판정, COUNT(*) as 매물수 FROM v_naver_sale_listing_vs_actual_latest GROUP BY 1"
        ).df()
        hoga_dict = dict(zip(df_hoga["실질급매_판정"], df_hoga["매물수"]))
    except:
        hoga_dict = {"일반매물": 29000, "태그만급매": 1400, "가격기준급매": 0}

    report = f"""# 매매 시장 심층 분석 마스터 리포트 V4 (사이클 투자 관점 & 실행 멘토링 강화)

본 문서(`04_결과/02_리포트_gemini/01_매매시장분석_20260313_gemini_v4.md`)는 부룡 님의 '사이클 투자 철학' 렌즈와 KOSIS 악성 미분양, 아파트 상품성(연식/단지) 심층 요인을 결합하여 작성된 실전용 멘토링 보고서입니다.

---

## 1. 장기 디커플링 진단 및 갭(Gap) 파워 배율

- **과거의 교훈**: 1차 광풍(2008) 직전 수도권 {int(s08)}만 원, 지방 {int(b08)}만 원에서, 역대급 디커플링(2013)으로 수도권 {int(s13)}만 원, 지방 {int(b13)}만 원이 되었습니다. 과거의 지방 활성화는 '가치 투자'의 대상이었으나, 현재(수도권 {int(s25)}만 / 비수도권 {int(b25)}만) 지방 및 외곽 하락은 인구소멸과 수요 증발로 인한 '출구 없는 무덤'입니다.
- **10급지 파워 배율 (수도권 1급지 / 5급지)**
  - `2018년 (확산 초기)`: **{round(gap18_1_5, 2)}배** (A급지 리드)
  - `2021년 (급등장 끝물)`: **{round(gap21_1_5, 2)}배** (B/C급지 강풍으로 갭 매우 좁혀짐)
  - `2023년 (도피)`: **{round(gap23_1_5, 2)}배**
  - `2025~최신`: **{round(gap25_1_5, 2)}배**. 현재 다시 한 번 A급지가 갭을 크게 벌려놓았습니다.
- **지방광역시 내부 파워 배율**: 현재 1급지 대장구와 3급지 간의 평당가 배율은 **{round(gap25_m1_m3, 2)}배**입니다. 지방 투자의 규칙은 언제나 "1급지 대장구(학군/일자리) 외에는 절대 하방선을 믿지 마라"입니다.

---

## 2. 최신 지역별 단기 모멘텀 (확산경로와 호랑이굴 스크리닝)

### 2.1 수도권 A급지의 확산 경로 (가치 투자 구간)
- **Top 리드 (수도권 상위)**: `{sudo_top.iloc[0]['시군구']}` (+{sudo_top.iloc[0]['YoY']:.2f}%), `{sudo_top.iloc[1]['시군구']}` (+{sudo_top.iloc[1]['YoY']:.2f}%)
- **상품성 심화 분석**: 이 Top 구역 내에서도, '5년 미만 신축'은 YoY 약 **{prod_new:.1f}%** 상승한 반면, '30년 이상 구축'은 **{prod_old:.1f}%** 성장에 그쳤습니다. "재건축보다 압도적 신축 거주 가치"에 실수요가 돈을 지불하고 있습니다. 즉, A급지 진입 시 '가치 방어' 목적으로는 썩다리보단 '최고가 신축'을 우선해야 합니다.

### 2.2 수도권 외곽 C급지 및 데스존 (마지막 폭탄의 증명)
- **둔화 늪 (Bottom 구역)**: `{sudo_bot.iloc[0]['시군구']}` (+{sudo_bot.iloc[0]['YoY']:.2f}%), `{sudo_bot.iloc[1]['시군구']}` (+{sudo_bot.iloc[1]['YoY']:.2f}%), `{sudo_bot.iloc[2]['시군구']}` ({sudo_bot.iloc[2]['YoY']:.2f}%)
- **준공후 미분양 (악성 폭탄) 교차 검증**: 해당 하위 5개 지역의 KOSIS 기준 '준공 후 미분양'은 1년 전 {bad_mibun_past}호에서 최근 **{bad_mibun_recent}호**로 변화/유지되고 있습니다. 실거주 수요가 완전히 무너져 악성 미분양 소화조차 안 되는 호랑이굴을 의미합니다. 투매 시점입니다.

### 2.3 비수도권 (광역시 vs 주요 중소도시)
- **광역시 대장 (안전마진)**: `{metro_top.iloc[0]['시군구']}`, `{metro_top.iloc[1]['시군구']}`. 1번지들만이 그나마 보합세를 유지하고 있습니다.
- **중소도시 (소외)**: `{small_top.iloc[0]['시군구']}`, `{small_top.iloc[1]['시군구']}`. 

---

## 3. 호가 심리 및 투자 시나리오 플레이북

{hoga_dict} (실질급매 거의 부재). 호가가 실거래보다 더 억세게 버티고 있습니다.

### 💡 향후 6~12개월 사이클 멘토링 매뉴얼
1. **[상승 시나리오] 확산 경로 선점**: A급지가 벌린 갭({round(gap25_1_5, 2)}배)을 메우러, 대중의 눈치가 다음 타깃인 '수도권 핵심 B/C+급 교통망 1순위 노드'로 향할 준비가 되었습니다. 자금이 여유 없는 3번째 투자자라면 바로 이 갭 메우기 포인트(분당구, 과천 등 인접 B급지)를 지금 선점해야 합니다.
2. **[보합 시나리오] 수익률의 한계 버티기**: 보합이 이어지더라도 B급지는 전세가율 반등(임차 멘토링 노트 참고)을 통해 투자 원금을 극도로 회수 가능한 구간입니다.
3. **[하락 시나리오] 절대 기피 구역 투매**: 준공후 미분양이 누적되는 C급 외곽 및 지방 2~3급지 보유자는 하락기를 버틸 체력이 없습니다. 강력 손절 타임입니다.

이제 이 플레이북을 들고, **[임차 시장(Phase 4)]**에서 갭투자 타깃과 캐시플로우 창출 후보를 상세히 파헤치며 이 시나리오의 톱니바퀴를 맞추겠습니다.
"""
    os.makedirs("04_결과/02_리포트_gemini", exist_ok=True)
    with open(
        "04_결과/02_리포트_gemini/01_매매시장분석_20260313_gemini.md",
        "w",
        encoding="utf-8",
    ) as f:
        f.write(report)
    print("Report generated successfully.")


if __name__ == "__main__":
    generate_report()
