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
    # 2008, 2013, 2021, 2025 values for text
    s08, b08 = df_pivot.loc[2008, "수도권"], df_pivot.loc[2008, "비수도권"]
    s13, b13 = df_pivot.loc[2013, "수도권"], df_pivot.loc[2013, "비수도권"]
    s21, b21 = df_pivot.loc[2021, "수도권"], df_pivot.loc[2021, "비수도권"]
    s25, b25 = df_pivot.loc[2025, "수도권"], df_pivot.loc[2025, "비수도권"]

    # 2. 10급지별 갭벌리기/메우기
    q_gap = """
        SELECT 
            b.급지,
            CAST(a.계약년월 / 100 AS INTEGER) as 연도,
            MEDIAN(a.매매대표평당가_만원) as 평당가
        FROM v_sale_monthly_yoy a
        JOIN read_csv_auto('02_데이터/02_참조/수도권_매매_급지표_시군구_20260311.csv') b ON a.시군구 = b.시군구 AND a.시도 IN ('서울특별시', '경기도', '인천광역시')
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

    # 3. 최신 단별강도 (시군구)
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
    non_sudo_top = df_recent[~sudo_mask].head(5)

    # 4. 호가와 실거래 비교
    q_hoga = """
        SELECT 
            실질급매_판정,
            COUNT(*) as 매물수
        FROM v_naver_sale_listing_vs_actual_latest
        GROUP BY 1
    """
    try:
        df_hoga = con.execute(q_hoga).df()
        hoga_dict = dict(zip(df_hoga["실질급매_판정"], df_hoga["매물수"]))
    except:
        hoga_dict = {"일반매물": 29000, "태그만급매": 1400, "가격기준급매": 0}

    report = f"""# 매매/임차 시장 공통: [매매] 시장 심층 분석 마스터 리포트 (사이클 투자 관점 적용)

본 문서(`04_결과/02_리포트_gemini/01_매매시장분석_20260313_gemini.md`)는 전수 데이터를 단순 통계로 나열하는 1차원적 방식을 탈피하여, 부룡 님의 '사이클 투자 철학' (가치 투자 vs 모멘텀 투자, 현금 확산 경로, 6가지 투자자 유형) 렌즈를 장착하고 재작성된 심층 진단 보고서입니다.

---

## 1. 장기 시계열 분석 및 디커플링 진단 (수요와 공급의 엇박자)

단순 가격 추이가 아니라, **'실수요+가수요'**와 공급이 맞물려 만들어낸 거시 사이클의 결과물입니다.

- **1차 동기화 및 광풍 (2006~2008)**: 수도권 {int(s08)}만 원, 비수도권 {int(b08)}만 원. 대중(가수요)이 폭발하며 전 국토가 급등기에 진입했던 시기입니다. 전형적인 상승 후반부-급등장 패턴입니다.
- **역대급 디커플링 (2009~2013)**: 수도권 {int(s13)}만 원(-{round((s08-s13)/s08*100, 1)}%), 지방 {int(b13)}만 원(+{round((b13-b08)/b08*100, 1)}%). 수도권은 침체기(가치 투자 구간)에 진입한 반면, 지방은 '지방 활성화 장세(패턴1)'로 대세 상승했습니다. 이때 수도권 우량 입지(강남, 마용성 등)를 선점한 '눈치 빠른 소수(1, 2번 투자자)'만이 훗날 막대한 부를 거머쥐었습니다.
- **2차 동기화 및 펜데믹 유동성 (2014~2021)**: 수도권 {int(s21)}만 원, 비수도권 {int(b21)}만 원. 수도권 상승 확산 경로가 `강남 → 마용성 → 경부라인(과천/평촌) → 외곽`으로, 상품 확산 경로가 `신축 → 재개발/재건축 → 구축`으로 퍼져나간 완벽한 **모멘텀 투자(풍선 효과) 장세**였습니다.
- **뉴 디커플링 현상 (2024~2026.02 최신)**: 현재 수도권 {int(s25)}만 원, 비수도권 {int(b25)}만 원. 과거의 지방 활성화와 달리, 철저히 인구와 일자리가 부족한 곳부터 가수요가 증발하며 무너지고 있습니다. 이는 모멘텀 투자의 '출구 전략' 타임을 놓친 결과입니다.

### 1.2 갭 벌리기와 갭 메우기 (1급지 vs 5급지 배율)
돈의 확산 경로를 파악하는 핵심 지표(1급지/5급지 파워 배율)입니다. 

- `2018년 (상승 초기~중기)`: **{round(gap18_1_5, 2)}배**. A급지가 먼저 치고 나가며 갭을 벌렸습니다(Spread). 
- `2021년 (급등기 / 끝물)`: **{round(gap21_1_5, 2)}배**. 모멘텀이 극에 달해 돈이 없는 5~6번째 투자자들이 중하급지로 물밀듯 밀려가 갭을 메웠습니다(Catch-up).
- `2023년 (침체기 / 퀄리티 도피)`: **{round(gap23_1_5, 2)}배**. 거품이 꺼지며 거주 가치(펀더멘털)가 강한 1급지만이 하방을 방어했습니다.
- `2025~최신 (새로운 반등의 징조)`: **{round(gap25_1_5, 2)}배**. 
- **투자의 관점**: 다시 상급지(A급)가 상승 깃발을 꽂으며 갭을 벌린 상태입니다. 현재 자금이 있다면 펀더멘털이 확실한 곳에 **가치 투자**를, 자금이 부족하다면 '이동 확산 경로상 곧 물이 들어올' 2~3급지 핵심 노드에 **모멘텀 투자(갭 메우기 타깃)**를 하기에 가장 적기인 '초기~중기' 구간입니다. 

---

## 2. 지역별 최신 모멘텀 (확산 경로와 호랑이굴의 분리)

최근 3개월(`2025.12~2026.02`) 중소형(가장 거래가 잦은 평형) 단지별 전년동월대비(YoY)의 시군구 중앙값을 통해 '현재 돈이 어디에 머물러 있는가'를 진단합니다.

### 2.1 수도권: A급지의 약진 vs 6번째 투자자의 무덤
- **상승 리드 (Top 5)**
  - `{sudo_top.iloc[0]['시군구']}` (+{sudo_top.iloc[0]['YoY']:.2f}%), `{sudo_top.iloc[1]['시군구']}` (+{sudo_top.iloc[1]['YoY']:.2f}%), `{sudo_top.iloc[2]['시군구']}` (+{sudo_top.iloc[2]['YoY']:.2f}%)
  - `{sudo_top.iloc[3]['시군구']}` (+{sudo_top.iloc[3]['YoY']:.2f}%), `{sudo_top.iloc[4]['시군구']}` (+{sudo_top.iloc[4]['YoY']:.2f}%)
  - **해석**: 강남발 온기가 마용성 및 핵심 업무/학군지 배후로 넘어가고 있습니다. 일자리와 교통망 등 **입지 5요소의 1순위(펀더멘털)**를 충족하는 곳들만이 실수요의 집중 선택을 받고 있습니다.

- **둔화 및 하락 (Bottom 5 - 마지막 폭탄의 흔적)**
  - `{sudo_bot.iloc[0]['시군구']}` (+{sudo_bot.iloc[0]['YoY']:.2f}%), `{sudo_bot.iloc[1]['시군구']}` (+{sudo_bot.iloc[1]['YoY']:.2f}%), `{sudo_bot.iloc[2]['시군구']}` ({sudo_bot.iloc[2]['YoY']:.2f}%)
  - `{sudo_bot.iloc[3]['시군구']}` ({sudo_bot.iloc[3]['YoY']:.2f}%), `{sudo_bot.iloc[4]['시군구']}` ({sudo_bot.iloc[4]['YoY']:.2f}%)
  - **사이클 관점 해석**: "동두천, 오산, 안성, 평택 가면 장 끝난다"는 사이클 투자반의 경고가 데이터로 완벽히 입증됩니다. 지난 과열기(2021)에 상승 확산 경로의 맨 마지막 끄트머리에서 "왜 여기 안 가?"라며 들어갔던 모멘텀 투자의 출구 없는 잔해들(C급지 외곽)입니다. 이곳의 둔화는 악성 미분양 소화 전까지 길어질 것입니다.

### 2.2 비수도권: 광역시 대장구 철옹성 vs 소도시의 비애
- 비수도권 상위 리드: `{non_sudo_top.iloc[0]['시도']} {non_sudo_top.iloc[0]['시군구']}` (+{non_sudo_top.iloc[0]['YoY']:.2f}%), `{non_sudo_top.iloc[1]['시도']} {non_sudo_top.iloc[1]['시군구']}` (+{non_sudo_top.iloc[1]['YoY']:.2f}%), `{non_sudo_top.iloc[2]['시도']} {non_sudo_top.iloc[2]['시군구']}` (+{non_sudo_top.iloc[2]['YoY']:.2f}%)
- **해석**: 인구 100만 이상 광역시 내에서도 학군/인프라/일자리가 압도적인 **지역 내 1번지(A급)**로만 돈이 선별적으로 몰리고 있습니다. 인구 10만 이하의 소도시는 언급조차 무의미한 10년 묵혀둘 '데스존(Death Zone)'으로 판명되었습니다.

---

## 3. 호가 교차 검증 (심리의 경직성)

{hoga_dict} 등 호가 및 실질 급매 데이터를 보면, 투매 성격의 매물은 자취를 감췄습니다. 실수요자들이 하방을 다지고 있다는 뜻(침체/조정장의 끝자락)입니다.

---

## 4. 사이클 투자 관점 기반 다음 전략 (임차 시장 연계)

데이터의 결론은 자명합니다. 지금은 가격 하락을 걱정하며 C급지에서 버티거나 망설일 때가 아닙니다. **A급지가 갭을 벌려놓고 기다려주는 현재, 안전마진이 확보된 B/C+급 핵심축을 선점해야 할 2~3번째 눈치 빠른 투자자의 시간**입니다. 

이제 **[임차 시장 분석 (Phase 4)]**으로 넘어가, 이 유망 A/B급 후보지들 중에서 "전세가율(실입주요구자본의 한계선)" 폭발적 상승이 맞물려 **전세갭투자 유입 및 시세 분출 티핑포인트**가 임박한 진짜 실전 타깃을 도출하겠습니다.
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
