import os
import json
import requests
import pandas as pd
import gspread
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials

API_URL = "https://apis.data.go.kr/B552845/katRealTime2/trades2"

def fetch_auction_data():
    api_key = os.getenv("KAT_API_KEY")
    base_date = "20260409"

    params = {
        "serviceKey": api_key,
        "pageNo": "1",
        "numOfRows": "1000",
        "type": "json",
        "basDt": base_date,
    }

    all_rows = []
    page = 1

    while True:
        params["pageNo"] = str(page)
        print(f"Page {page} 요청 중... (날짜: {base_date})")
        resp = requests.get(API_URL, params=params, timeout=30)
        print(f"HTTP Status: {resp.status_code}")

        if resp.status_code != 200:
            print(f"API 오류: {resp.text[:300]}")
            break

        data = resp.json()
        body = data.get("response", {}).get("body", {})
        total_count = int(body.get("totalCount", 0))
        print(f"totalCount: {total_count}")

        if total_count == 0:
            print("데이터 없음")
            break

        items = body.get("items", {})
        item_list = items.get("item", []) if isinstance(items, dict) else items
        if isinstance(item_list, dict):
            item_list = [item_list]

        all_rows.extend(item_list)
        print(f"Page {page}: {len(item_list)}건 수집")

        if page * int(params["numOfRows"]) >= total_count:
            break
        page += 1

    print(f"총 {len(all_rows)}건 수집 완료")
    return all_rows

def push_to_sheets(rows):
    gcp_json_str = os.getenv("GCP_JSON")
    if not gcp_json_str:
        raise ValueError("GCP_JSON 환경변수가 없습니다!")

    credentials_dict = json.loads(gcp_json_str)
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    client = gspread.authorize(creds)

    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    if not spreadsheet_id:
        raise ValueError("SPREADSHEET_ID 환경변수가 없습니다!")

    spreadsheet = client.open_by_key(spreadsheet_id)
    worksheet = spreadsheet.sheet1

    if not rows:
        print("적재할 데이터 없음 — 종료")
        return

    df = pd.DataFrame(rows)

    existing = worksheet.get_all_values()
    if not existing:
        worksheet.append_row(df.columns.tolist())

    worksheet.append_rows(df.astype(str).values.tolist())
    print(f"{len(rows)}건 Google Sheets 적재 완료!")

if __name__ == "__main__":
    rows = fetch_auction_data()
    push_to_sheets(rows)
