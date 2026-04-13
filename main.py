import os
import json
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

# 1. 설정
API_KEY = os.getenv('API_KEY')
GCP_JSON = os.getenv('GCP_JSON')
ENDPOINT = 'https://apis.data.go.kr/B552845/katRealTime2'
SHEET_NAME = '도매시장_경매데이터_수집'

def fetch_data(target_date):
    print(f">>> {target_date} 날짜 데이터 수집 시도 중...")
    params = {
        'serviceKey': API_KEY,
        'delngDe': target_date,
        'pageNo': '1',
        'numOfRows': '100',
        'type': 'json'
    }
    try:
        response = requests.get(ENDPOINT, params=params, timeout=30)
        res_json = response.json()
        items = res_json.get('response', {}).get('body', {}).get('items', [])
        
        if not items:
            # 만약 items가 리스트가 아니라 딕셔너리 하나인 경우 대응
            item_data = res_json.get('response', {}).get('body', {}).get('items', {}).get('item', [])
            return pd.DataFrame(item_data) if isinstance(item_data, list) else pd.DataFrame([item_data]) if item_data else pd.DataFrame()
            
        return pd.DataFrame(items)
    except Exception as e:
        print(f"API 호출 에러 ({target_date}): {e}")
        return pd.DataFrame()

def main():
    # 오늘과 어제 데이터를 모두 시도 (데이터가 없는 경우 대비)
    today = datetime.now().strftime('%Y%m%d')
    yesterday = (datetime.now() - timedelta(1)).strftime('%Y%m%d')
    
    df_today = fetch_data(today)
    df_yesterday = fetch_data(yesterday)
    df_total = pd.concat([df_yesterday, df_today])

    if df_total.empty:
        print("!!! 수집된 데이터가 전혀 없습니다. API 응답을 확인해주세요.")
        return

    try:
        print(">>> 구글 시트 연결 시도 중...")
        creds_dict = json.loads(GCP_JSON)
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        
        sh = client.open(SHEET_NAME)
        # 'data'라는 이름의 탭을 명시적으로 찾음
        try:
            worksheet = sh.worksheet("data")
        except:
            worksheet = sh.get_worksheet(0)
            
        # 데이터프레임의 결측치를 빈 문자열로 처리 (에러 방지)
        df_total = df_total.fillna("")
        data_list = df_total.values.tolist()
        
        worksheet.append_rows(data_list)
        print(f"✅ 성공: {len(data_list)}건의 데이터를 시트에 추가했습니다.")
        
    except Exception as e:
        print(f"❌ 구글 시트 작업 중 치명적 에러: {e}")

if __name__ == "__main__":
    main()
