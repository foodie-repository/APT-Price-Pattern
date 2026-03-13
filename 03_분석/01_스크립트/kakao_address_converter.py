import os
import time
import requests
import pandas as pd
import duckdb

#####################################################################
# 카카오 로컬 API 기반 (백그라운드용) 지번주소 -> 도로명주소 변환 스크립트
#####################################################################


def get_kakao_road_address(query, api_key):
    """지번주소를 도로명주소로 변환, 실패시 None 반환"""
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    headers = {"Authorization": f"KakaoAK {api_key}"}
    params = {"query": query}
    try:
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code == 200:
            docs = resp.json().get("documents", [])
            if docs:
                doc = docs[0]
                if doc.get("road_address"):
                    return doc["road_address"].get("address_name")
                elif doc.get("address"):
                    return doc["address"].get("address_name")
    except Exception as e:
        pass
    return None


def main():
    api_key = os.getenv("KAKAO_REST_API_KEY")
    if not api_key:
        try:
            with open(".env", "r") as f:
                for line in f:
                    if line.startswith("KAKAO_REST_API_KEY="):
                        api_key = line.strip().split("=", 1)[1].strip("\"'")
        except FileNotFoundError:
            pass

    if not api_key:
        print("에러: KAKAO_REST_API_KEY 환경변수나 .env 파일이 설정되어야 합니다.")
        print("export KAKAO_REST_API_KEY='당신의_키'")
        return

    con = duckdb.connect("02_데이터/03_가공/analysis.duckdb")
    source_db = "/Volumes/T9/duckdb-analytics/db/apartment.duckdb"
    con.execute(f"ATTACH IF NOT EXISTS '{source_db}' AS src (READ_ONLY)")

    # 처리할 주소 추출 (기존 매핑내역 제외)
    query = """
        SELECT DISTINCT a.지번주소 
        FROM t_sale_monthly_px a
        LEFT JOIN src.좌표 b ON a.지번주소 = b.도로명주소 
        WHERE b.도로명주소 IS NULL AND a.지번주소 IS NOT NULL
    """

    try:
        # 이전에 처리하다 멈춘 내역이 있다면 제외
        con.execute(
            "CREATE TABLE IF NOT EXISTS address_mapping (지번주소 VARCHAR, 매핑_도로명주소 VARCHAR)"
        )

        query_filtered = f"""
            SELECT src.지번주소 
            FROM ({query}) src
            LEFT JOIN address_mapping m ON src.지번주소 = m.지번주소
            WHERE m.지번주소 IS NULL
        """
        todo_df = con.execute(query_filtered).df()

        total = len(todo_df)
        print(f"변환 대상 주소 총 {total}건")

        if total == 0:
            print("모든 주소가 처리되었습니다.")
            return

        # 100건 단위로 백그라운드 저장
        batch_size = 100
        batch_results = []

        for idx, row in todo_df.iterrows():
            jibeon = row["지번주소"]
            road_addr = get_kakao_road_address(jibeon, api_key)
            time.sleep(0.05)  # 카카오 초당 요청 수 제약 고려

            batch_results.append((jibeon, road_addr))

            if len(batch_results) >= batch_size or idx == total - 1:
                # DB에 바로 삽입 (에러 발생시에도 최소한 복구되도록)
                con.executemany(
                    "INSERT INTO address_mapping VALUES (?, ?)", batch_results
                )
                print(f"진행 상황: {idx+1} / {total} 완료")
                batch_results = []

    except Exception as e:
        print(f"작업 중 오류 발생: {e}")


if __name__ == "__main__":
    main()
