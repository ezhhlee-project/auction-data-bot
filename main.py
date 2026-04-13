import os
import json
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# GitHub Secrets에서 정보 가져오기
API_KEY = os.getenv('API_KEY')
GCP_JSON = os.getenv('GCP_JSON')
ENDPOINT = 'https://apis.data.go.kr/B552845/katRealTime2'
SHEET_NAME = '도매시장_경매데이터_수집'

def fetch_today_data():
    today_str = datetime.now().strftime('%Y%m%d')
    all_data = []
    # ... (기존 API 호출 로직과 동일) ...
    return pd.DataFrame(all_data)

def upload_to_gsheets(df):
    if df.empty: return
    
    # JSON 문자열을 딕셔너리로 변환하여 인증
    creds_dict = json.loads(GCP_JSON)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    sh = client.open(SHEET_NAME)
    worksheet = sh.get_worksheet(0)
    worksheet.append_rows(df.values.tolist())

if __name__ == "__main__":
    df = fetch_today_data()
    upload_to_gsheets(df)
