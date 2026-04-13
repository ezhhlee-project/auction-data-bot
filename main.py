import os
import json
from datetime import datetime, timedelta
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


def parse_kst_datetime(dt_str: str):
    """
    API의 날짜/시간 문자열을 KST datetime으로 변환
    """
    if not dt_str:
        return None

    dt_str = str(dt_str).strip()
    kst = ZoneInfo("Asia/Seoul")

    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y%m%d%H%M%S",
        "%Y%m%d%H%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
    ]

    for fmt in formats:
        try:
            parsed = datetime.strptime(dt_str, fmt)
            return parsed.replace(tzinfo=kst)
        except ValueError:
            continue

    return None


def get_target_dates(start_dt: datetime, end_dt: datetime):
    """
    최근 1시간 범위가 걸치는 거래정산일자 목록 반환
    예:
      - 12:00 실행 -> ['2026-04-13']
      - 00:10 실행 -> ['2026-04-12', '2026-04-13']
    """
    dates = []
    current_date = start_dt.date()

    while current_date <= end_dt.date():
        dates.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)

    return dates


def fetch_auction_data_for_date(api_key: str, target_date: str):
    """
    특정 거래정산일자(trd_clcln_ymd)에 대한 전체 데이터 조회
    """
    num_of_rows = 1000
    page_no = 1
    all_items = []

    print(f"조회 날짜 시작: {target_date}")

    while True:
        params = {
            "serviceKey": api_key,
            "pageNo": str(page_no),
            "numOfRows": str(num_of_rows),
            "returnType": "json",
            "cond[trd_clcln_ymd::EQ]": target_date,
        }

        resp = requests.get(API_URL, params=params, timeout=30)
        print(f"[{target_date}][page {page_no}] HTTP Status: {resp.status_code}")
        print(f"[{target_date}][page {page_no}] 응답 내용 (앞 300자): {resp.text[:300]}")
        resp.raise_for_status()

        if "json" not in resp.headers.get("Content-Type", "").lower():
            raise ValueError("JSON 응답이 아닙니다. 서비스키나 파라미터를 확인하세요.")

        data = resp.json()
        response = data.get("response", {})
        header = response.get("header", {})
        body = response.get("body", {})

        result_code = str(header.get("resultCode"))
        result_msg = header.get("resultMsg")
        print(f"[{target_date}][page {page_no}] resultCode: {result_code}, resultMsg: {result_msg}")

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

        print(f"[{target_date}][page {page_no}] 원본 수집 건수: {len(item_list)} / totalCount: {total_count}")

        if not item_list:
            break

        all_items.extend(item_list)

        if len(item_list) < num_of_rows:
            break

        page_no += 1

    print(f"조회 날짜 종료: {target_date}, 총 원본 건수: {len(all_items)}")
    return all_items


def fetch_auction_data():
    api_key = os.getenv("KAT_API_KEY")
    if not api_key:
        raise ValueError("KAT_API_KEY 환경변수가 없습니다!")

    kst = ZoneInfo("Asia/Seoul")
    now_kst = datetime.now(kst)
    one_hour_ago = now_kst - timedelta(hours=1)

    print("API 호출 시작...")
    print(f"수집 시각(KST): {now_kst.strftime('%Y-%m-%d %H:%M:%S')}")
    print(
        f"낙찰일시 허용 범위(KST): "
        f"{one_hour_ago.strftime('%Y-%m-%d %H:%M:%S')} ~ {now_kst.strftime('%Y-%m-%d %H:%M:%S')}"
    )

    target_dates = get_target_dates(one_hour_ago, now_kst)
    print(f"조회 대상 거래정산일자: {target_dates}")

    raw_items = []
    for target_date in target_dates:
        raw_items.extend(fetch_auction_data_for_date(api_key, target_date))

    # 중복 제거: 경매고유번호 + 낙찰일시 기준
    dedup_map = {}
    for item in raw_items:
        key = (
            str(item.get("auctn_seq", "")).strip(),
            str(item.get("scsbd_dt", "")).strip(),
        )
        dedup_map[key] = item

    deduped_items = list(dedup_map.values())
    print(f"중복 제거 후 건수: {len(deduped_items)}")

    filtered_items = []
    for item in deduped_items:
        middle_category = str(item.get("gds_mclsf_nm", "")).strip()
        scsbd_dt_raw = item.get("scsbd_dt", "")
        scsbd_dt = parse_kst_datetime(scsbd_dt_raw)

        # 조건 1) 상품중분류명 whitelist
        if middle_category not in ALLOWED_MIDDLE_CATEGORIES:
            continue

        # 조건 2) 낙찰일시가 최근 1시간 이내
        if scsbd_dt is None:
            continue

        if not (one_hour_ago <= scsbd_dt <= now_kst):
            continue

        filtered_items.append(item)

    print(f"최종 필터 적용 후 총 {len(filtered_items)}건 수집 완료")
    return filtered_items


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
