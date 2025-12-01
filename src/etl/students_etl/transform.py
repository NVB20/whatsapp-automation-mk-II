import os
from datetime import datetime
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
import gspread

from src.etl.db.mongodb.mongo_handler import get_mongo_connection
from src.sheets_connect import init_google_sheets

load_dotenv()

# Configuration from .env
SHEET_ID = os.getenv('SHEET_ID')
PRACTICE_WORDS = os.getenv('PRACTICE_WORDS', '').split(',')
MESSAGE_WORDS = os.getenv('MESSAGE_WORDS', '').split(',')

# Clean up whitespace from words
PRACTICE_WORDS = [word.strip() for word in PRACTICE_WORDS if word.strip()]
MESSAGE_WORDS = [word.strip() for word in MESSAGE_WORDS if word.strip()]


def normalize_phone_number(phone: str) -> str:
    """
    Normalize phone number by removing special characters and formatting consistently.
    Handles formats like:
    - '+972 55-660-2298' -> '972 55-660-2298'
    - '⁦+972 55-660-2298⁩' -> '972 55-660-2298'
    """
    if not phone:
        return ''
    
    # Remove invisible Unicode characters (left-to-right marks, zero-width spaces, etc.)
    phone = ''.join(char for char in phone if char.isprintable())
    
    # Remove the leading + if present
    phone = phone.lstrip('+').strip()
    
    return phone


def contains_keyword(text: str, keywords: List[str]) -> bool:
    """Check if text contains any of the keywords."""
    return any(keyword in text for keyword in keywords)


def determine_message_type(text: str) -> Optional[str]:
    """Determine message type based on keyword match."""
    if contains_keyword(text, PRACTICE_WORDS):
        return 'practice'
    elif contains_keyword(text, MESSAGE_WORDS):
        return 'message'
    return None


def get_students_from_sheets() -> Dict[str, Dict[str, str]]:
    """
    Fetch student data from Google Sheets (main worksheet).
    Returns a dictionary with phone as key for fast lookup.
    """
    client = init_google_sheets()
    
    if not client:
        print("Failed to initialize Google Sheets client")
        return {}
    
    try:
        SHEET_NAME = 'main'
        
        # Open the spreadsheet and get the worksheet
        spreadsheet = client.open_by_key(SHEET_ID)
        worksheet = spreadsheet.worksheet(SHEET_NAME)
        
        # Get all values from the worksheet
        rows = worksheet.get_all_values()
        
        if not rows:
            print("No data found in Google Sheets")
            return {}
        
        students_dict = {}
        
        # Skip header row, process data rows
        for row in rows[1:]:
            if len(row) < 5:  # Ensure row has enough columns
                continue
                
            # A: phone, B: name, C: lesson, E: teacher (index 4)
            phone = row[0].strip() if row[0] else ''
            
            if not phone:
                continue
            
            # Normalize phone number
            phone = normalize_phone_number(phone)
            
            # Extract lesson number from "שיעור num" format
            # Handle cases like "שיעור 12שיעור 12שיעור 9" - take only the first occurrence
            lesson_raw = row[2].strip() if len(row) > 2 and row[2] else ''
            
            # Find the first occurrence of "שיעור" and extract the number after it
            if 'שיעור' in lesson_raw:
                # Split by 'שיעור' and get the second part (first number)
                parts = lesson_raw.split('שיעור')
                # Get the first non-empty part after 'שיעור'
                for part in parts[1:]:
                    # Extract only digits from the beginning of the part
                    lesson_number = ''.join(c for c in part if c.isdigit())
                    if lesson_number:
                        break
                else:
                    lesson_number = ''
            else:
                lesson_number = ''
            
            students_dict[phone] = {
                'name': row[1].strip() if len(row) > 1 and row[1] else '',
                'lesson': lesson_number,
                'teacher': row[4].strip() if len(row) > 4 and row[4] else ''
            }
        
        print(f"Successfully loaded {len(students_dict)} students from Google Sheets")
        return students_dict
        
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Error: Spreadsheet with ID '{SHEET_ID}' not found")
        return {}
    except gspread.exceptions.WorksheetNotFound:
        print(f"Error: Worksheet '{SHEET_NAME}' not found in spreadsheet")
        return {}
    except Exception as e:
        print(f"Error reading from Google Sheets: {e}")
        import traceback
        traceback.print_exc()
        return {}


def get_last_message_or_practice(stats_collection, phone_number: str, message_type: str) -> Optional[datetime]:
    """
    Get the last message or practice timestamp for a student from MongoDB stats.
    
    Args:
        stats_collection: MongoDB collection for student stats
        phone_number: Student's phone number
        message_type: 'message' or 'practice'
    
    Returns:
        Last timestamp or None if not found
    """
    student_stat = stats_collection.find_one({'phone_number': phone_number})
    
    if not student_stat:
        return None
    
    # Get the appropriate field based on message type
    if message_type == 'practice':
        return student_stat.get('last_practice')
    elif message_type == 'message':
        return student_stat.get('last_message')
    
    return None


def transform(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Transform extracted messages into the format needed for loading.
    
    Input: List of message dicts from extract (with fields: phone, text, timestamp, etc.)
    
    Output: List of dicts with format:
        {
            'message_type': 'practice' or 'message',
            'phone_number': str,
            'name': str (from sheets),
            'lesson': str (from sheets),
            'teacher': str (from sheets),
            'current_timestamp': datetime (from current message),
            'last_message': datetime or None (from mongo) - only if message_type is 'message',
            'last_practice': datetime or None (from mongo) - only if message_type is 'practice'
        }
    """
    print(f"{'='*60}")
    print(f"Starting transform for {len(messages)} messages")
    print(f"{'='*60}")

    
    # Get student data from Google Sheets
    students_dict = get_students_from_sheets()
    
    if not students_dict:
        print("No students found in Google Sheets - cannot transform")
        return []
    
    # Get MongoDB connection for stats lookup
    mongo_conn = get_mongo_connection()
    stats_collection = mongo_conn.get_students_stats_collection()
    
    transformed_records = []
    
    for msg in messages:
        # Try multiple possible field names for phone number
        phone_number = (
            msg.get('phone', '') or 
            msg.get('phone_number', '') or 
            msg.get('from', '') or
            msg.get('sender', '') or
            ''
        ).strip()
        
        # Normalize phone number to match sheets format
        phone_number = normalize_phone_number(phone_number)
        
        if not phone_number:
            print(f"Warning: Message missing phone field. Available fields: {list(msg.keys())}")
            continue
        
        text = msg.get('text', '')
        current_timestamp = msg.get('timestamp')
        
        if not current_timestamp:
            print(f"Warning: Message missing timestamp - skipping")
            continue
        
        # Determine message type based on keywords
        message_type = determine_message_type(text)
        
        if not message_type:
            # Skip messages that don't match any keywords
            continue
        
        # Check if student exists in sheets
        if phone_number not in students_dict:
            print(f"Warning: Phone '{phone_number}' not found in Google Sheets - skipping")
            continue
        
        # Get student info from sheets
        student_info = students_dict[phone_number]
        
        # Get the relevant last timestamp from MongoDB based on message type
        last_timestamp = get_last_message_or_practice(stats_collection, phone_number, message_type)
        
        # Build transformed record with only the relevant last_ field
        transformed_record = {
            'message_type': message_type,
            'phone_number': phone_number,
            'name': student_info['name'],
            'lesson': student_info['lesson'],
            'teacher': student_info['teacher'],
            'current_timestamp': current_timestamp
        }
        
        # Add only the relevant last_ field based on message type
        if message_type == 'message':
            transformed_record['last_message'] = last_timestamp
        elif message_type == 'practice':
            transformed_record['last_practice'] = last_timestamp
        
        transformed_records.append(transformed_record)
        
        print(f"✓ Transformed: {student_info['name']} ({phone_number}) - Type: {message_type}")
    
    print(f"\n{'='*60}")
    print(f"Transform complete: {len(transformed_records)} records")
    print(f"{'='*60}\n")
    
    return transformed_records


def update_student_stats(stats_collection, phone_number: str, message_type: str, timestamp: datetime):
    """
    Update the last message or practice timestamp for a student.
    
    Args:
        stats_collection: MongoDB collection for student stats
        phone_number: Student's phone number
        message_type: 'message' or 'practice'
        timestamp: The timestamp to update
    """
    update_field = 'last_message' if message_type == 'message' else 'last_practice'
    
    stats_collection.update_one(
        {'phone_number': phone_number},
        {
            '$set': {
                'phone_number': phone_number,
                update_field: timestamp,
                'updated_at': datetime.now()
            }
        },
        upsert=True
    )
    print(f"Updated stats for {phone_number}: {update_field} = {timestamp}")


# Example usage for testing
if __name__ == '__main__':
    # Simulated extract data (this would come from your extract phase)
    sample_messages = [
        {
            'phone': '0501234567',
            'text': 'שלחתי הודעה למורה',
            'timestamp': datetime.now()
        },
        {
            'phone': '0507654321',
            'text': 'העלתי תרגול לתיקייה',
            'timestamp': datetime.now()
        }
    ]
    
    # Run transform
    transformed_data = transform(sample_messages)
    
    # Print results
    if transformed_data:
        print("\nTransformed records:")
        for i, record in enumerate(transformed_data, 1):
            print(f"\nRecord {i}:")
            for key, value in record.items():
                print(f"  {key}: {value}")
    else:
        print("\nNo records were transformed")