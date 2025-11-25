import os
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials

# Load environment variables
load_dotenv()

# Google Sheets configuration
credentials_file = os.getenv("CREDENTIALS_FILE")


def init_google_sheets():
    """Initialize Google Sheets connection"""
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    
    try:            
        creds = Credentials.from_service_account_file(credentials_file, scopes=scopes)
        client = gspread.authorize(creds)
        print(f"✅ Successfully connected to Google Sheets")
        return client
        
    except Exception as e:
        print(f"❌ Error initializing Google Sheets: {e}")
        import traceback
        traceback.print_exc()
        return None