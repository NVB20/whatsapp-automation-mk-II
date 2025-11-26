from src.etl.sales_etl.load import upload_leads_to_sheets
from src.etl.sales_etl.transform import process_sales_messages

def run_sales_etl(sales_messages):    
    try:
  
        # Process leads / Transform
        leads = process_sales_messages(sales_messages)
        
        if not leads:
            print("⚠ No leads extracted from messages")
            return {"success": 0, "errors": ["No valid leads found"]}
        
        # Upload to sheets
        result = upload_leads_to_sheets(leads)
        
        print(f"✓ ETL Complete: {result['success']} leads uploaded")
        return result
        
    except Exception as e:
        print(f"✗ ETL Failed: {e}")
        import traceback
        traceback.print_exc()
        return {"success": 0, "errors": [str(e)]}

    