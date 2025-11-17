#get messages from whatsapp
# make the code work with not only israel +972
#read sheets, get phone, name and lesson 
#filter messages into clean 2 dicts (practice/messages)
#connect from sheets if phone --> name || if name --> phone and add lesson
#jsonify the messages
# end goal: filtered dicts messges: {phone number, name, type(message/practice), lesson, teacher, last messge, }

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
        init_google_sheets()
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
                
                # Debug: Print cleaned phone
                if idx <= 5:
                    print(f"  Cleaned phone: '{cleaned_phone}'")
                
                # Also map by name for reverse lookup
                if name:
                    name_to_data[name.lower()] = student_data
        
        print(f"\nLoaded {len(phone_to_data)} students from Google Sheets")
        print(f"Sample phone numbers in mapping: {list(phone_to_data.keys())[:3]}")
        return phone_to_data, name_to_data
    
    except Exception as e:
        print(f"Error loading Google Sheets data: {e}")
        import traceback
        traceback.print_exc()
        return {}, {}


def clean_phone_number(phone: str) -> str:
    """Clean phone number to format: 972 52-299-1474"""
    # Remove unicode direction marks and other special characters
    cleaned = re.sub(r'[\u2066\u2069\u200e\u200f\s+]', '', phone)
    
    # Extract digits only
    digits = re.sub(r'\D', '', cleaned)
    
    # Format: 972 52-299-1474 (country code, then XX-XXX-XXXX)
    if digits.startswith('972'):
        # Already has country code
        if len(digits) == 12:  # 972 + 9 digits
            return f"{digits[:3]} {digits[3:5]}-{digits[5:8]}-{digits[8:]}"
    
    return digits  # Return as-is if format doesn't match


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


def process_messages(messages: List[Dict]) -> Dict[str, List[Dict]]:
    """
    Process messages and separate into categories
    
    Returns:
        Dict with 'practice' and 'message' lists containing enriched message data
    """
    # Load student data from Google Sheets
    phone_to_data, name_to_data = load_student_data()
    
    practice_messages = []
    message_sent = []
    
    for msg in messages:
        # Get student info from Google Sheets
        student_info = extract_student_info(msg['sender'], phone_to_data, name_to_data)
        
        # Determine message type
        msg_type = None
        if contains_keyword(msg['text'], PRACTICE_WORDS):
            msg_type = "practice"
        elif contains_keyword(msg['text'], MESSAGE_WORDS):
            msg_type = "message"
        
        if msg_type:
            # Create enriched message dict
            enriched_msg = {
                'phone_number': student_info['phone'],
                'name': student_info['name'],
                'type': msg_type,
                'lesson': student_info['lesson'],
                'teacher': student_info['teacher'],
                'last_message': msg['text'],
                'timestamp': msg['timestamp']
            }
            
            if msg_type == "practice":
                practice_messages.append(enriched_msg)
            else:
                message_sent.append(enriched_msg)
    
    print(f"=== {len(practice_messages)} Practice Messages Found ===")    
    print(f"=== {len(message_sent)} Messages Sent Found ===")
    
    return {
        'practice': practice_messages,
        'message': message_sent
    }



    
