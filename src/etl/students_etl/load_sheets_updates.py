import os
from datetime import datetime
from typing import List, Dict, Any
from dotenv import load_dotenv

from src.sheets_connect import init_google_sheets

# Load environment variables
load_dotenv()

SHEET_ID = os.getenv('SHEET_ID')
WORKSHEET_NAME = "main"


def parse_timestamp(timestamp_str: str) -> datetime:
    """
    Parse timestamp string to datetime object.
    Handles multiple formats:
    - '18:51, 12/4/2025' (24-hour format with M/D/YYYY)
    - '6:51 PM, 12/4/2025' (12-hour format with AM/PM)
    - 'HH:MM, DD.MM.YYYY' (24-hour format with D.M.YYYY)
    """
    formats = [
        '%H:%M, %m/%d/%Y',      # '18:51, 12/4/2025'
        '%I:%M %p, %m/%d/%Y',   # '6:51 PM, 12/4/2025'
        '%H:%M, %d.%m.%Y',      # 'HH:MM, DD.MM.YYYY'
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue
    
    # If none of the formats worked, raise an error
    print(f"Error parsing timestamp '{timestamp_str}': Does not match any known format")
    raise ValueError(f"Could not parse timestamp: {timestamp_str}")


def update_practice_dates(transformed_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Update Google Sheets with the latest practice date for students who practiced.
    Only updates students who have practice records in the current batch.
    
    Args:
        transformed_records: List of transformed records from ETL
        
    Returns:
        Dictionary with update statistics
    """
    if not transformed_records:
        print("No records to update in Google Sheets")
        return {
            'students_updated': 0,
            'students_not_found': 0,
            'errors': 0
        }
    
    print(f"{'='*60}")
    print(f"Updating Google Sheets with practice dates")
    print(f"{'='*60}")
    
    # Initialize Google Sheets connection
    try:
        client = init_google_sheets()
        if not client:
            raise Exception("Failed to initialize Google Sheets client")
        
        spreadsheet = client.open_by_key(SHEET_ID)
        sheet = spreadsheet.worksheet(WORKSHEET_NAME)
    except Exception as e:
        print(f"✗ Failed to connect to Google Sheets: {e}")
        import traceback
        traceback.print_exc()
        return {
            'students_updated': 0,
            'students_not_found': 0,
            'errors': 1
        }
    
    # Filter only practice records and get the latest practice per student
    student_practices = {}
    
    for record in transformed_records:
        if record['message_type'] == 'practice':
            phone_number = record['phone_number']
            current_timestamp_str = record['current_timestamp']
            
            # Parse timestamp string to datetime for proper comparison
            try:
                current_timestamp = parse_timestamp(current_timestamp_str)
            except Exception as e:
                print(f"⚠ Could not parse timestamp '{current_timestamp_str}': {e}")
                continue
            
            # Keep only the latest practice per student
            if phone_number not in student_practices:
                student_practices[phone_number] = {
                    'record': record,
                    'timestamp': current_timestamp
                }
            else:
                existing_timestamp = student_practices[phone_number]['timestamp']
                if current_timestamp > existing_timestamp:
                    student_practices[phone_number] = {
                        'record': record,
                        'timestamp': current_timestamp
                    }
    
    if not student_practices:
        print("No practice records found in this batch")
        return {
            'students_updated': 0,
            'students_not_found': 0,
            'errors': 0
        }
    
    print(f"Found {len(student_practices)} students with practice records")
    
    # Get all data from sheet (assuming headers in row 1)
    try:
        all_data = sheet.get_all_values()
        headers = all_data[0] if all_data else []
        rows = all_data[1:] if len(all_data) > 1 else []
    except Exception as e:
        print(f"✗ Failed to read sheet data: {e}")
        return {
            'students_updated': 0,
            'students_not_found': 0,
            'errors': 1
        }
    
    # Validate headers
    expected_headers = ['phone_number', 'name', 'lesson', 'last_practice']
    if not all(h in headers for h in expected_headers):
        print(f"✗ Sheet headers don't match expected format")
        print(f"  Expected: {expected_headers}")
        print(f"  Found: {headers}")
        return {
            'students_updated': 0,
            'students_not_found': 0,
            'errors': 1
        }
    
    # Find column indices
    phone_col_idx = headers.index('phone_number')
    last_practice_col_idx = headers.index('last_practice')
    
    # Statistics
    stats = {
        'students_updated': 0,
        'students_not_found': 0,
        'errors': 0
    }
    
    # Build a map of phone numbers to row indices
    phone_to_row = {}
    for idx, row in enumerate(rows):
        if len(row) > phone_col_idx:
            phone_number = row[phone_col_idx].strip()
            phone_to_row[phone_number] = idx + 2  # +2 because: 0-indexed to 1-indexed, plus header row
    
    # Update each student's last practice date
    updates = []
    
    for phone_number, practice_data in student_practices.items():
        try:
            practice_record = practice_data['record']
            practice_timestamp = practice_data['timestamp']
            
            if phone_number not in phone_to_row:
                print(f"⚠ Student not found in sheet: {practice_record['name']} ({phone_number})")
                stats['students_not_found'] += 1
                continue
            
            # Get row number (1-indexed for Google Sheets)
            row_num = phone_to_row[phone_number]
            
            # Format as DD/MM/YYYY (date only)
            practice_date = practice_timestamp.strftime('%d/%m/%Y')
            
            # Prepare cell update
            cell_address = f"{chr(65 + last_practice_col_idx)}{row_num}"  # Convert to A1 notation
            updates.append({
                'range': cell_address,
                'values': [[practice_date]]
            })
            
            print(f"✓ Queued update: {practice_record['name']} ({phone_number}) - {practice_date} at {cell_address}")
            stats['students_updated'] += 1
            
        except Exception as e:
            print(f"✗ Error processing {phone_number}: {e}")
            import traceback
            traceback.print_exc()
            stats['errors'] += 1
    
    # Batch update all cells at once
    if updates:
        try:
            sheet.batch_update(updates)
            print(f"✓ Successfully updated {len(updates)} cells in Google Sheets")
        except Exception as e:
            print(f"✗ Failed to batch update Google Sheets: {e}")
            import traceback
            traceback.print_exc()
            stats['errors'] += len(updates)
            stats['students_updated'] = 0
    
    print(f"{'='*60}")
    print(f"Google Sheets update complete:")
    print(f"  Students updated: {stats['students_updated']}")
    print(f"  Students not found in sheet: {stats['students_not_found']}")
    print(f"  Errors: {stats['errors']}")
    print(f"{'='*60}")
    
    return stats