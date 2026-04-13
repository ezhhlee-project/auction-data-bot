import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

API_URL = "https://apis.data.go.kr/B552845/katRealTime2/trades2"

# 수집 대상 상품중분류명만 허용
ALLOWED_MIDDLE_CATEGORIES = {
    "노지감귤",
    "한라봉",
    "레드향",
    "천혜향",
    "카라향",
    "당근",
    "무",
    "양배추",
    "마늘",
    "양파",
    "브로콜리",
}


def fetch_auction_data():
    api_key = os.getenv("KAT_API_KEY")
    if not api_key:
        raise ValueError("KAT_API_KEY 환경변수가 없습니다!")

    target_date = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")
    num_of_rows = 1000
    page_no = 1
    all_items = []

    print("API 호출 시작...")
    print(f"조회 날짜: {target_date}")

    while True:
        params = {
            "serviceKey": api_key,
            "pageNo": str(page_no),
            "numOfRows": str(num_of_rows),
            "returnType": "json",
            "cond[trd_clcln_ymd::EQ]": target_date,
        }

        resp = requests.get(API_URL, params=params, timeout=30)
        print(f"[page {page_no}] HTTP Status: {resp.status_code}")
        print(f"[page {page_no}] 응답 내용 (앞 300자): {resp.text[:300]}")
        resp.raise_for_status()

        if "json" not in resp.headers.get("Content-Type", "").lower():
            raise ValueError("JSON 응답이 아닙니다. 서비스키나 파라미터를 확인하세요.")

        data = resp.json()
        response = data.get("response", {})
        header = response.get("header", {})
        body = response.get("body", {})

        result_code = str(header.get("resultCode"))
        result_msg = header.get("resultMsg")
        print(f"[page {page_no}] resultCode: {result_code}, resultMsg: {result_msg}")

        if result_code not in ("00", "0", "INFO-000"):
            raise ValueError(f"API 오류: {result_code} / {result_msg}")

        total_count = int(body.get("totalCount", 0))
        items = body.get("items", [])

        if isinstance(items, dict):
            item_list = items.get("item", [])
        else:
            item_list = items

        if isinstance(item_list, dict):
            item_list = [item_list]
        elif item_list is None:
            item_list = []

        print(f"[page {page_no}] 원본 수집 건수: {len(item_list)} / totalCount: {total_count}")

        if not item_list:
            break

        # 상품중분류명(gds_mclsf_nm) 기준 필터링
        filtered_items = [
            item for item in item_list
            if str(item.get("gds_mclsf_nm", "")).strip() in ALLOWED_MIDDLE_CATEGORIES
        ]

        print(
            f"[page {page_no}] 필터 후 건수: {len(filtered_items)} "
            f"(허용 품목: {', '.join(sorted(ALLOWED_MIDDLE_CATEGORIES))})"
        )

        all_items.extend(filtered_items)

        # 원본 데이터 기준 total_count를 사용하므로
        # 페이지 순회 종료 조건은 item_list 기준으로 판단
        if len(item_list) < num_of_rows:
            break

        page_no += 1

    print(f"필터 적용 후 총 {len(all_items)}건 수집 완료")
    return all_items


def push_to_sheets(rows):
    if not rows:
        print("적재할 데이터 없음 — 종료")
        return

    collected_at = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")

    enriched_rows = []
    for row in rows:
        new_row = dict(row)
        new_row["collected_at"] = collected_at
        enriched_rows.append(new_row)

    headers = [
        "collected_at",
        "auctn_seq",
        "corp_cd",
        "corp_gds_cd",
        "corp_gds_item_nm",
        "corp_gds_vrty_nm",
        "corp_nm",
        "gds_lclsf_cd",
        "gds_lclsf_nm",
        "gds_mclsf_cd",
        "gds_mclsf_nm",
        "gds_sclsf_cd",
        "gds_sclsf_nm",
        "mdfcn_dt",
        "pkg_cd",
        "pkg_nm",
        "plor_cd",
        "plor_nm",
        "qty",
        "scsbd_dt",
        "scsbd_prc",
        "spm_no",
        "trd_clcln_ymd",
        "trd_se",
        "unit_cd",
        "unit_nm",
        "unit_qty",
        "whsl_mrkt_cd",
        "whsl_mrkt_nm",
    ]

    header_names_kr = [
        "수집시각",
        "경매고유번호",
        "법인코드",
        "법인상품코드",
        "법인상품품목명",
        "법인상품품종명",
        "법인명",
        "상품대분류코드",
        "상품대분류명",
        "상품중분류코드",
        "상품중분류명",
        "상품소분류코드",
        "상품소분류명",
        "수정일시 또는 변경일시",
        "포장코드",
        "포장명",
        "원산지코드",
        "원산지명",
        "수량",
        "낙찰일시",
        "단량당 낙찰가(원)",
        "원표번호",
        "거래정산일자",
        "매매방법",
        "단위코드",
        "단위명",
        "단위물량",
        "도매시장코드",
        "도매시장명",
    ]

    df = pd.DataFrame([{col: row.get(col, "") for col in headers} for row in enriched_rows])

    gcp_json_str = os.getenv("GCP_JSON")
    if not gcp_json_str:
        raise ValueError("GCP_JSON 환경변수가 없습니다!")

    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    if not spreadsheet_id:
        raise ValueError("SPREADSHEET_ID 환경변수가 없습니다!")

    credentials_dict = json.loads(gcp_json_str)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = Credentials.from_service_account_info(credentials_dict, scopes=scopes)
    client = gspread.Client(auth=creds)

    print("Spreadsheet 연결 중...")
    spreadsheet = client.open_by_key(spreadsheet_id)
    worksheet = spreadsheet.sheet1

    existing = worksheet.get_all_values()
    if not existing:
        worksheet.append_row(header_names_kr)

    worksheet.append_rows(df.astype(str).values.tolist())
    print(f"{len(rows)}건 Google Sheets 적재 완료! 수집시각: {collected_at}")


if __name__ == "__main__":
    rows = fetch_auction_data()
    push_to_sheets(rows)
