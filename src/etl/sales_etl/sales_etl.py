from datetime import datetime, timedelta
from src.etl.sales_etl.load import upload_leads_to_sheets
from src.etl.sales_etl.transform import process_sales_messages, format_leads_for_sheets
from src.etl.db.mongodb.mongo_handler import get_mongo_connection


def log_sales_run(new_leads, run_timestamp, total_run_time, success=True, error_message=None):
    """
    Log sales ETL run statistics to logger_stats collection
    
    Args:
        new_leads: Number of new leads found
        run_timestamp: Timestamp when the run started
        total_run_time: Total execution time in seconds
        success: Whether the run was successful
        error_message: Error message if run failed
    """
    try:
        mongo = get_mongo_connection()
        logger_collection = mongo.get_logger_stats_collection()
        
        log_entry = {
            "source": "sales_etl",
            "log_level": "info" if success else "error",
            "timestamp": run_timestamp,
            "new_leads": new_leads,
            "total_run_time": round(total_run_time, 2),
            "success": success,
            "error_message": error_message,
            "metadata": {
                "process": "sales_lead_extraction",
                "run_date": run_timestamp.strftime("%Y-%m-%d"),
                "run_time": run_timestamp.strftime("%H:%M:%S")
            }
        }
        
        logger_collection.insert_one(log_entry)
        print(f"✓ Logged sales run: {new_leads} leads in {total_run_time:.2f}s")
        
    except Exception as e:
        print(f"⚠ Could not log to logger_stats: {e}")


def run_sales_etl(sales_messages, use_test_data=False):
    """
    Run the sales ETL process with logging to logger_stats.
    
    Args:
        sales_messages: List of sales messages to process
        use_test_data: If True, uses test messages instead of real data
    """
    run_timestamp = datetime.now()
    start_time = datetime.now()
    new_leads_count = 0
    
    try:        
        # Process leads / Transform
        leads = process_sales_messages(sales_messages)
        
        if not leads:
            print("⚠ No leads extracted from messages")
            total_run_time = (datetime.now() - start_time).total_seconds()
            
            # Log run with 0 leads
            log_sales_run(
                new_leads=0,
                run_timestamp=run_timestamp,
                total_run_time=total_run_time,
                success=True,
                error_message="No valid leads found"
            )
            
            return {"success": 0, "errors": ["No valid leads found"]}
        
        new_leads_count = len(leads)
        
        # Format leads for sheets (do this once here)
        formatted_leads = format_leads_for_sheets(leads)
        
        # Upload to sheets (pass already formatted leads)
        result = upload_leads_to_sheets(formatted_leads)
        
        # Calculate total run time
        total_run_time = (datetime.now() - start_time).total_seconds()
        
        # Log successful run
        log_sales_run(
            new_leads=new_leads_count,
            run_timestamp=run_timestamp,
            total_run_time=total_run_time,
            success=True
        )
        
        print(f"✓ ETL Complete: {result['success']} leads uploaded in {total_run_time:.2f}s")
        return result
        
    except Exception as e:
        # Calculate run time even on error
        total_run_time = (datetime.now() - start_time).total_seconds()
        
        # Log failed run
        log_sales_run(
            new_leads=new_leads_count,
            run_timestamp=run_timestamp,
            total_run_time=total_run_time,
            success=False,
            error_message=str(e)
        )
        
        print(f"✗ ETL Failed: {e}")
        import traceback
        traceback.print_exc()
        return {"success": 0, "errors": [str(e)]}