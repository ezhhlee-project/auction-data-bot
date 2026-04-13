import os
import json
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def push_to_sheets():
    try:
        print("--- Step 1: Loading Credentials ---")
        # Load the GCP_JSON secret from environment variables
        gcp_json_str = os.getenv('GCP_JSON')
        if not gcp_json_str:
            raise ValueError("GCP_JSON environment variable is missing!")
        
        credentials_dict = json.loads(gcp_json_str)
        
        # Define scope and authorize
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
        client = gspread.authorize(creds)

        print("--- Step 2: Preparing Data ---")
        # Example: Replace this with your actual data scraping/generation logic
        # For now, creating a dummy DataFrame to ensure it works
        data = {
            'Date': [pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')],
            'Status': ['Success'],
            'Source': ['GitHub Actions Bot']
        }
        df = pd.DataFrame(data)

        print("--- Step 3: Accessing Spreadsheet ---")
        # Replace 'Your_Spreadsheet_Name' with the actual name of your Google Sheet
        # Make sure you have shared the sheet with the email in your GCP_JSON
        sheet = client.open('Your_Spreadsheet_Name').get_workflow_sheet(0) 

        print("--- Step 4: Pushing Data ---")
        # Append the data to the end of the sheet
        sheet.append_rows(df.values.tolist())
        print("Successfully pushed data to Google Sheets!")

    except Exception as e:
        print(f"FAILED: {str(e)}")
        import traceback
        traceback.print_exc()
        exit(1)

if __name__ == "__main__":
    push_to_sheets()
