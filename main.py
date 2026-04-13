import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

API_URL = "https://apis.data.go.kr/B552845/katRealTime2/trades2"


def fetch_auction_data():
    api_key = os.getenv("KAT_API_KEY")
    if not api_key:
        raise ValueError("KAT_API_KEY 환경변수가 없습니다!")

    target_date = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")

    params = {
        "serviceKey": api_key,
        "pageNo": "1",
        "numOfRows": "100",
        "returnType": "json",
        "cond[trd_clcln_ymd::EQ]": target_date,
    }

    print("API 호출 중...")
    print(f"조회 날짜: {target_date}")

    resp = requests.get(API_URL, params=params, timeout=30)
    print(f"HTTP Status: {resp.status_code}")
    print(f"응답 내용 (앞 500자): {resp.text[:500]}")

    resp.raise_for_status()

    if "json" not in resp.headers.get("Content-Type", "").lower():
        raise ValueError("JSON 응답이 아닙니다. 서비스키나 파라미터를 확인하세요.")

    data = resp.json()
    response = data.get("response", {})
    header = response.get("header", {})
    body = response.get("body", {})

    result_code = str(header.get("resultCode"))
    result_msg = header.get("resultMsg")
    print(f"resultCode: {result_code}, resultMsg: {result_msg}")

    if result_code not in ("00", "0", "INFO-000"):
        raise ValueError(f"API 오류: {result_code} / {result_msg}")

    total_count = int(body.get("totalCount", 0))
    print(f"totalCount: {total_count}")

    if total_count == 0:
        print("데이터 없음")
        return []

    items = body.get("items", [])
    if isinstance(items, dict):
        item_list = items.get("item", [])
    else:
        item_list = items

    if isinstance(item_list, dict):
        item_list = [item_list]

    print(f"총 {len(item_list)}건 수집 완료")
    return item_list


def push_to_sheets(rows):
    if not rows:
        print("적재할 데이터 없음 — 종료")
        return

    collected_at = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")

    # 완전 로그형: 기존 데이터 비교 없이 이번 수집 시각만 붙여서 전부 append
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
