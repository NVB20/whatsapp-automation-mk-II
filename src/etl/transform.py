#get messages from whatsapp
#read sheets, get phone and lesson 
#filter messages into clean 2 dicts (practice/messages)
#connect lesson to message json
#jsonify the messages

import re
import os
from dotenv import load_dotenv
from typing import Dict, List

# Load environment variables
load_dotenv()

# Get search words from environment and parse them
practice_search_word = os.getenv("PRACTICE_WORDS", "")
message_search_word = os.getenv("MESSAGE_WORDS", "")

# Parse the strings into lists (remove brackets and quotes, split by comma)
PRACTICE_WORDS = [word.strip().strip('"').strip("'") 
                  for word in practice_search_word.strip('[]').split(',') 
                  if word.strip()]
MESSAGE_WORDS = [word.strip().strip('"').strip("'") 
                 for word in message_search_word.strip('[]').split(',') 
                 if word.strip()]


def clean_phone_number(phone: str) -> str:
    """
    Clean phone number to format: 972 52-299-1474
    Removes unicode characters and formats properly
    """
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


def process_messages(messages: List[Dict]) -> tuple[List[Dict], List[Dict]]:
    """
    Process messages and separate into practice_messages and message_sent
    
    Returns:
        tuple: (practice_messages, message_sent)
    """
    practice_messages = []
    message_sent = []
    
    for msg in messages:
        # Clean phone number
        cleaned_phone = clean_phone_number(msg['sender'])
        
        # Create cleaned message dict
        cleaned_msg = {
            'sender': cleaned_phone,
            'timestamp': msg['timestamp'],
            'text': msg['text']
        }
        
        # Filter based on keywords
        if contains_keyword(msg['text'], PRACTICE_WORDS):
            practice_messages.append(cleaned_msg)
        elif contains_keyword(msg['text'], MESSAGE_WORDS):
            message_sent.append(cleaned_msg)


    print(f"===",len(practice_messages), "Practice Messages Found ===",)    
    print(f"===" ,len(message_sent), "Messages Sent Found ===")
    
    return practice_messages, message_sent


if __name__ == "__main__":
   
    
    practice_messages, message_sent = process_messages(sample_messages)
    
