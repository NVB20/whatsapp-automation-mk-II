#get messages from whatsapp
# make the code work with not only israel +972
#read sheets, get phone, name and lesson 
#filter messages into clean 2 dicts (practice/messages)
#connect from sheets if phone --> name || if name --> phone and add lesson
#jsonify the messages
# end goal: filtered dicts messges: {phone number, name, type(message/practice), lesson, teacher, last messge, }
#remove last message

import re
import os
from dotenv import load_dotenv
from typing import Dict, List
from src.sheets_connect import init_google_sheets

# Load environment variables
load_dotenv()

# Get search words from environment and parse them
practice_search_word = os.getenv("PRACTICE_WORDS", "")
message_search_word = os.getenv("MESSAGE_WORDS", "")
sheet_id = os.getenv("SHEET_ID")

# Parse the strings into lists (remove brackets and quotes, split by comma)
PRACTICE_WORDS = [word.strip().strip('"').strip("'") 
                  for word in practice_search_word.strip('[]').split(',') 
                  if word.strip()]
MESSAGE_WORDS = [word.strip().strip('"').strip("'") 
                 for word in message_search_word.strip('[]').split(',') 
                 if word.strip()]


def load_student_data() -> tuple[Dict[str, Dict], Dict[str, Dict]]:
    """
    Load student data from Google Sheets
    Sheet columns: A=phone number, B=name, C=status, E=teacher
    
    Returns:
        tuple: (phone_to_data dict, name_to_data dict)
    """
    try:
        client = init_google_sheets()
        if not client:
            print("Failed to initialize Google Sheets client")
            return {}, {}
        
        sheet = client.open_by_key(sheet_id)
        worksheet = sheet.worksheet("main")
        
        # Get all values
        all_values = worksheet.get_all_values()
        
        print(f"Total rows in sheet: {len(all_values)}")
        print(f"First row (header): {all_values[0] if all_values else 'Empty'}")
        
        phone_to_data = {}
        name_to_data = {}
        
        # Skip header row
        for idx, row in enumerate(all_values[1:], start=2):
            if not row or len(row) < 2:  # Need at least phone and name
                continue
                
            phone = row[0].strip() if len(row) > 0 and row[0] else ""
            name = row[1].strip() if len(row) > 1 and row[1] else ""
            status = row[2].strip() if len(row) > 2 and row[2] else ""
            teacher = row[4].strip() if len(row) > 4 and row[4] else ""
            
            # Debug: Print first few rows
            if idx <= 5:
                print(f"Row {idx}: phone='{phone}', name='{name}', status='{status}', teacher='{teacher}'")
            
            # Clean phone number
            if phone:
                cleaned_phone = clean_phone_number(phone)
                student_data = {
                    'phone': cleaned_phone,
                    'name': name,
                    'lesson': status,
                    'teacher': teacher
                }
                phone_to_data[cleaned_phone] = student_data
                
                # Also map by name for reverse lookup
                if name:
                    name_to_data[name.lower()] = student_data
        
        print(f"Loaded {len(phone_to_data)} students from Google Sheets")
        print(f"Sample phone numbers in mapping: {list(phone_to_data.keys())[:3]}")
        return phone_to_data, name_to_data
    
    except Exception as e:
        print(f"Error loading Google Sheets data: {e}")
        import traceback
        traceback.print_exc()
        return {}, {}


import re

def clean_phone_number(phone: str) -> str:
    """
    Clean and normalize phone numbers.
    - Removes Unicode direction marks, spaces, symbols.
    - Detects Israeli numbers and formats: 972 52-299-1474
    - For other countries: returns +<digits> (E.164 style)
    """

    # Remove direction marks, whitespace, and invisible characters
    cleaned = re.sub(r'[\u2066\u2069\u200e\u200f\s]', '', phone)

    # Fix "+" if inserted between characters like "+ 972"
    cleaned = cleaned.replace("+", "")

    # Keep only digits
    digits = re.sub(r'\D', "", cleaned)

    # If empty → return empty
    if not digits:
        return ""

    # Local IL number (10 digits starting with 05X)
    if len(digits) == 10 and digits.startswith("05"):
        intl = "972" + digits[1:]
        return f"{intl[:3]} {intl[3:5]}-{intl[5:8]}-{intl[8:]}"  # 972 52-299-1474

    # Already international Israeli (starts with 972)
    if digits.startswith("972") and len(digits) == 12:
        return f"{digits[:3]} {digits[3:5]}-{digits[5:8]}-{digits[8:]}"


    return digits



def contains_keyword(text: str, keywords: List[str]) -> bool:
    """Check if text contains any of the keywords"""
    text_lower = text.lower()
    return any(keyword.lower() in text_lower for keyword in keywords)


def extract_student_info(sender: str, phone_to_data: Dict, name_to_data: Dict) -> Dict:
    """
    Extract student information from sender
    If sender is phone - add name, if sender is name - add phone
    
    Returns:
        Dict with phone, name, lesson, and teacher
    """
    # Try as phone number first
    cleaned_phone = clean_phone_number(sender)
    
    print(f"Looking up sender: '{sender}' -> cleaned: '{cleaned_phone}'")
    
    if cleaned_phone in phone_to_data:
        print(f"  Found in phone_to_data!")
        return phone_to_data[cleaned_phone]
    
    # Try as name
    sender_lower = sender.lower().strip()
    if sender_lower in name_to_data:
        print(f"  Found in name_to_data!")
        return name_to_data[sender_lower]
    
    print(f"  Not found in sheets data")
    # Not found - return basic info
    return {
        'phone': cleaned_phone if cleaned_phone else sender,
        'name': sender if not cleaned_phone else "Unknown",
        'lesson': "Unknown",
        'teacher': "Unknown"
    }


def process_messages(messages: List[Dict]) -> List[Dict]: 
    """
    Process messages and enrich with student data
    
    Returns:
        List of enriched messages ready for database
    """
    # Load student data from Google Sheets
    phone_to_data, name_to_data = load_student_data()
    
    processed_messages = []
    practice_count = 0
    message_count = 0
    
    for msg in messages:
        # Get student info from Google Sheets
        student_info = extract_student_info(msg['sender'], phone_to_data, name_to_data)
        
        # Determine message type
        msg_type = None
        if contains_keyword(msg['text'], PRACTICE_WORDS):
            msg_type = "practice"
            practice_count += 1
        elif contains_keyword(msg['text'], MESSAGE_WORDS):
            msg_type = "message"
            message_count += 1
        
        if msg_type:
            # Create enriched message dict with ALL required fields
            enriched_msg = {
                # Required fields for MongoDB
                'phone_number': student_info['phone'],
                'timestamp': msg['timestamp'],
                'content': msg['text'],  # IMPORTANT: Add the actual message text!
                
                # Optional enrichment fields
                'name': student_info['name'],
                'message_category': msg_type,  # 'practice' or 'message'
                'lesson': student_info['lesson'],
            }
            
            processed_messages.append(enriched_msg)
    
    print(f"=== {practice_count} Practice Messages Found ===")    
    print(f"=== {message_count} Messages Sent Found ===")
    
    return processed_messages  