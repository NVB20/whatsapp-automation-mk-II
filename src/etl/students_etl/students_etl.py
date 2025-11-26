from src.etl.transform import process_messages
from src.etl.db.mongodb.message_saver import MessageSaver
from src.etl.students_etl.sheets_updater import update_sheets_from_mongo

def run_students_etl(students_messages):
    # Transform
    students_messages = process_messages(students_messages)
    print(f"Processed {len(students_messages)} messages")

    # Load (with deduplication)
    try:
        saver = MessageSaver()
        results = saver.save_messages_batch(students_messages)
        
        # Print results
        print("=" * 60 + "\n" + "DATABASE SAVE RESULTS" + "\n" + "=" * 60 )
        print(f"Total processed:     {results['total']}")
        print(f"✓ New messages:      {results['inserted']}")
        print(f"⊘ Duplicates skipped: {results['skipped']}")
        print(f"✗ Errors:            {results['errors']}")
        print("=" * 60)
        
        # Update Google Sheets with latest practice dates
        if results['inserted'] > 0:
            print("Updating Google Sheets with latest practice dates...")
            sheets_results = update_sheets_from_mongo()
            
            if sheets_results:
                print(f"{len(students_messages)} messages scanned")
                print(f"✓ Sheets updated: {sheets_results['updates_needed']} students")
        else:
            print("⊘ No new messages - skipping Sheets update")
        
        return results
        
    except Exception as e:
        print("=" * 60)
        print("DATABASE SAVE FAILED")
        print("=" * 60)
        print(f"Error: {e}")
        raise