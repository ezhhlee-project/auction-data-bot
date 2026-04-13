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

    result_code = header.get("resultCode")
    result_msg = header.get("resultMsg")
    print(f"resultCode: {result_code}, resultMsg: {result_msg}")

    if result_code not in ("00", "INFO-000"):
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

    print(f"Spreadsheet 연결 중... ID: {spreadsheet_id}")
    spreadsheet = client.open_by_key(spreadsheet_id)
    worksheet = spreadsheet.sheet1

    df = pd.DataFrame(rows)

    existing = worksheet.get_all_values()
    if not existing:
        worksheet.append_row(df.columns.tolist())

    worksheet.append_rows(df.astype(str).values.tolist())
    print(f"{len(rows)}건 Google Sheets 적재 완료!")


if __name__ == "__main__":
    rows = fetch_auction_data()
    push_to_sheets(rows)
