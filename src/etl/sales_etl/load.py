import os
from src.etl.sales_etl.transform import format_leads_for_sheets
from src.sheets_connect import init_google_sheets
from dotenv import load_dotenv

# Load environment variables once at module level
load_dotenv()

def find_next_empty_row(sheet, column='B'):
    """
    Find the next empty row by checking column B.
    Column A is reserved for checkboxes.
    """
    # Get all values from column B
    col_values = sheet.col_values(2)  # Column B is index 2
    
    # Find the first empty cell (next available row)
    # Add 1 because list is 0-indexed but sheets rows start at 1
    next_row = len(col_values) + 1
    
    print(f"Next available row: {next_row}")
    return next_row





def upload_leads_to_sheets(leads):
    """
    Upload leads to Google Sheets starting from the next available row.
    """
    if not leads:
        print("No leads to upload.")
        return {"success": 0, "errors": []}
    
    sheet = get_sales_worksheet()

    # Find starting row
    start_row = find_next_empty_row(sheet)
    
    # Format all leads for upload
    formatted_leads = [format_leads_for_sheets(lead) for lead in leads]
    
    # Prepare the range (B:F for columns B through F)
    end_row = start_row + len(formatted_leads) - 1
    cell_range = f'B{start_row}:F{end_row}'
    
    print(f"\nUploading {len(formatted_leads)} leads to range {cell_range}...")
    
    try:
        # Batch update - much more efficient than row-by-row
        sheet.update(cell_range, formatted_leads)
        
        print(f"✓ Successfully uploaded {len(formatted_leads)} leads!")
        print(f"  Rows {start_row} to {end_row} updated")
        
        return {
            "success": len(formatted_leads),
            "errors": [],
            "start_row": start_row,
            "end_row": end_row
        }
        
    except Exception as e:
        print(f"✗ Error uploading to sheets: {str(e)}")
        return {
            "success": 0,
            "errors": [str(e)]
        }
    
def get_sales_worksheet():
    """
    Get the sales worksheet connection.
    Separated for reusability and testing.
    """
    sheet_id = os.getenv("SALES_SHEET_ID")
    
    if not sheet_id:
        raise ValueError("SALES_SHEET_ID not found in environment variables")
    
    client = init_google_sheets()
    spreadsheet = client.open_by_key(sheet_id)
    worksheet = spreadsheet.worksheet("main")
    
    print(f"✓ Connected to sales spreadsheet")
    return worksheet   
