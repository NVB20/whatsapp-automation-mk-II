import os
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials

# Load environment variables
load_dotenv()

# Google Sheets configuration
sheet_id = os.getenv("SHEET_ID")
CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "secrets/sheets-api-cred.json")


def init_google_sheets():
    """Initialize Google Sheets connection"""
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets.readonly',
        'https://www.googleapis.com/auth/drive.readonly'
    ]
    
    try:
        # Check if credentials file exists
        if not os.path.exists(CREDENTIALS_FILE):
            print(f"❌ Credentials file not found at: {CREDENTIALS_FILE}")
            print(f"📁 Current working directory: {os.getcwd()}")
            print(f"🔍 Looking for file at: {os.path.abspath(CREDENTIALS_FILE)}")
            
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        print(f"✅ Successfully connected to Google Sheets")
        return client
        
    except Exception as e:
        print(f"❌ Error initializing Google Sheets: {e}")
        import traceback
        traceback.print_exc()
        return None