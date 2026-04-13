def fetch_data(target_date):
    print(f">>> {target_date} 날짜 데이터 수집 시도 중...")
    
    # 인증키를 URL에 직접 넣지 않고 params로 전달하되, 
    # API에 따라 인코딩된 키와 디코딩된 키 중 하나만 작동하는 경우가 있습니다.
    params = {
        'serviceKey': API_KEY, # 인증키
        'delngDe': target_date,
        'pageNo': '1',
        'numOfRows': '100',
        'type': 'json'
    }
    
    try:
        # 1. API 호출
        response = requests.get(ENDPOINT, params=params, timeout=30)
        
        # 로그 확인: 응답 코드가 200이 아니면 문제 있음
        if response.status_code != 200:
            print(f"❌ API 서버 응답 코드 에러: {response.status_code}")
            return pd.DataFrame()

        # 2. 응답 내용 확인 (JSON이 아닌 에러 메시지가 올 경우 대비)
        content = response.text.strip()
        if not content:
            print("❌ API 응답 내용이 비어 있습니다.")
            return pd.DataFrame()
        
        if content.startswith('<'): # XML/HTML 에러 메시지가 온 경우
            print(f"❌ API가 JSON 대신 XML/HTML을 반환함: {content[:100]}...")
            return pd.DataFrame()

        res_json = response.json()
        
        # 3. 데이터 추출 (구조가 유동적일 수 있음)
        body = res_json.get('response', {}).get('body', {})
        items = body.get('items', [])
        
        # items가 딕셔너리 {'item': [...]} 형태인 경우 처리
        if isinstance(items, dict):
            items = items.get('item', [])
        
        # 데이터가 하나만 있을 때 딕셔너리로 오는 경우 리스트로 변환
        if isinstance(items, dict):
            items = [items]
            
        if not items:
            print(f"ℹ️ {target_date} 데이터가 아직 없습니다.")
            return pd.DataFrame()
            
        print(f"✅ {target_date} 데이터 수집 완료 ({len(items)}건)")
        return pd.DataFrame(items)

    except Exception as e:
        print(f"❌ API 처리 중 에러 발생: {str(e)}")
        return pd.DataFrame()
