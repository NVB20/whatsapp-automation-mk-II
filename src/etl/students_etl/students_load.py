import os
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv
from src.sheets_connect import init_google_sheets
from src.etl.db.mongodb.message_saver import MessageSaver
from src.etl.transform import clean_phone_number

load_dotenv()

SHEET_ID = os.getenv("SHEET_ID")

# Fixed column indices (0-based)
PHONE_COL = 0      # Column A: phone number
NAME_COL = 1       # Column B: name
LESSON_COL = 2     # Column C: lesson
PRACTICE_COL = 3   # Column D: last practice
TEACHER_COL = 4    # Column E: teacher


class SheetsUpdater:
    """
    Updates Google Sheets with latest practice and message dates
    """
    
    def __init__(self):
        """Initialize Google Sheets connection"""
        self.client = init_google_sheets()
        if not self.client:
            raise Exception("Failed to initialize Google Sheets client")
        
        self.sheet = self.client.open_by_key(SHEET_ID)
        self.worksheet = self.sheet.worksheet("main")
        
        print(f"✓ Connected to Google Sheets")
    
    def get_current_dates_from_sheets(self) -> Dict[str, Dict]:
        """
        Get current practice dates from Google Sheets
        """
        all_values = self.worksheet.get_all_values()
        phone_to_data = {}
        
        # Skip header row
        for row_idx, row in enumerate(all_values[1:], start=2):
            if not row or len(row) <= PHONE_COL:
                continue
            
            phone = row[PHONE_COL].strip() if len(row) > PHONE_COL else ""
            name = row[NAME_COL].strip() if len(row) > NAME_COL else ""
            practice_date = row[PRACTICE_COL].strip() if len(row) > PRACTICE_COL else ""
            
            if phone:
                cleaned_phone = clean_phone_number(phone)
                
                phone_to_data[cleaned_phone] = {
                    'practice': practice_date,
                    'name': name,
                    'row': row_idx
                }
        
        print(f"✓ Loaded data for {len(phone_to_data)} students from sheets")
        return phone_to_data
    
    def parse_date(self, date_str: str) -> Optional[datetime]:
        """
        Parse date string in various formats
        """
        if not date_str or not date_str.strip():
            return None
        
        date_str = date_str.strip()
        
        # Try different date formats
        formats = [
            '%H:%M, %d.%m.%Y',       # 21:16, 23.11.2025 (WhatsApp format with time)
            '%d.%m.%Y',              # 23.11.2025
            '%d/%m/%y',              # 20/11/25
            '%d/%m/%Y',              # 20/11/2025
            '%Y-%m-%d',              # 2025-11-20
            '%Y-%m-%dT%H:%M:%S',     # 2025-11-20T10:00:00
            '%d.%m.%y',              # 20.11.25
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        print(f"⚠️ Could not parse date: '{date_str}'")
        return None
    
    def format_date_for_sheets(self, date_obj: datetime) -> str:
        """
        Format datetime object for Google Sheets (dd/mm/yy)
        """
        return date_obj.strftime('%d/%m/%y')
    
    def get_latest_practice_dates_from_mongo(self) -> Dict[str, datetime]:
        """
        Get latest practice date per phone number from MongoDB
        """
        saver = MessageSaver()
        collection = saver.collection
        
        # Aggregation pipeline to get latest practice date per phone
        pipeline = [
            {
                '$match': {
                    'message_category': 'practice'
                }
            },
            {
                '$group': {
                    '_id': '$phone_number',
                    'latest_timestamp': {'$max': '$timestamp'}
                }
            }
        ]
        
        results = list(collection.aggregate(pipeline))
        
        phone_to_date = {}
        
        for result in results:
            phone = result['_id']
            timestamp_str = result['latest_timestamp']
            
            # Parse timestamp
            timestamp = self.parse_date(timestamp_str)
            if timestamp:
                phone_to_date[phone] = timestamp
        
        print(f"✓ Found latest practice dates for {len(phone_to_date)} students in MongoDB")
        return phone_to_date
    
    def update_practice_dates(self) -> Dict:
        """
        Update Google Sheets column D (last practice) with latest dates from MongoDB
        """
        print("=" * 60)
        print("Updating Google Sheets Dashboard (MongoDB is Master)")
        print("=" * 60)
        
        # Get current dates from sheets
        current_data = self.get_current_dates_from_sheets()
        
        # Get latest dates from MongoDB (MASTER DATA)
        latest_dates = self.get_latest_practice_dates_from_mongo()
        
        batch_updates = []  # For batch API call
        stats = {
            'total_checked': len(current_data),
            'updates_needed': 0,
            'no_changes': 0,
            'not_in_mongo': 0,
            'students_updated': []
        }
        
        for phone, sheet_data in current_data.items():
            if phone not in latest_dates:
                stats['not_in_mongo'] += 1
                continue
            
            mongo_date = latest_dates[phone]
            row_num = sheet_data['row']
            
            # MongoDB is master - always sync to MongoDB's date
            new_date_str = self.format_date_for_sheets(mongo_date)
            
            # Only update if different
            if sheet_data['practice'] != new_date_str:
                # Prepare batch update entry
                # Range format: 'D2' for row 2, column D (no sheet name needed)
                cell_range = f'D{row_num}'
                
                batch_updates.append({
                    'range': cell_range,
                    'values': [[new_date_str]]
                })
                
                stats['updates_needed'] += 1
                stats['students_updated'].append({
                    'name': sheet_data['name'],
                    'phone': phone,
                    'old': sheet_data['practice'] or 'Empty',
                    'new': new_date_str
                })
                
                print(f"  📅 {sheet_data['name']} ({phone})")
                print(f"     {sheet_data['practice'] or 'Empty'} → {new_date_str}")
            else:
                stats['no_changes'] += 1
        
        # Apply updates in SINGLE BATCH
        if batch_updates:
            print(f"Applying {len(batch_updates)} updates in BATCH...")
            
            try:
                # Use batch_update for efficiency (single API call)
                self.worksheet.batch_update(batch_updates)
                print(f"✓ Batch update successful! ({len(batch_updates)} cells updated)")
            except Exception as e:
                print(f"✗ Batch update failed: {e}")
                raise
        
        elif not batch_updates:
            print(f"✓ All dates are synchronized with MongoDB!")
        
        # Print summary
        print("=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"Total students checked: {stats['total_checked']}")
        print(f"✓ Updates applied: {stats['updates_needed']}")
        print(f"⊘ Already synced: {stats['no_changes']}")
        print(f"⚠ Not in MongoDB: {stats['not_in_mongo']}")
        print("=" * 60)
        
        return stats
    
    def get_student_practice_history(self, phone: str, limit: int = 10) -> List[Dict]:
        """
        Get practice history for a specific student
        """
        saver = MessageSaver()
        collection = saver.collection
        
        cleaned_phone = clean_phone_number(phone)
        
        results = collection.find({
            'phone_number': cleaned_phone,
            'message_category': 'practice'
        }).sort('timestamp', -1).limit(limit)
        
        history = []
        for record in results:
            history.append({
                'date': record.get('timestamp'),
                'content': record.get('content', 'N/A')[:50] + '...',
                'teacher': record.get('teacher', 'Unknown')
            })
        
        return history
    
    def print_update_preview(self, phone: str):
        """
        Preview what would be updated for a specific student
        """
        cleaned_phone = clean_phone_number(phone)
        
        # Get from sheets
        current_data = self.get_current_dates_from_sheets()
        sheet_info = current_data.get(cleaned_phone)
        
        if not sheet_info:
            print(f"Phone {cleaned_phone} not found in Google Sheets")
            return
        
        # Get from MongoDB
        latest_dates = self.get_latest_practice_dates_from_mongo()
        mongo_date = latest_dates.get(cleaned_phone)
        
        print("=" * 60)
        print(f"Preview for {sheet_info['name']} ({cleaned_phone})")
        print("=" * 60)
        print(f"Current in sheets: {sheet_info['practice'] or 'Empty'}")
        print(f"Latest in MongoDB: {self.format_date_for_sheets(mongo_date) if mongo_date else 'No practices found'}")
        
        if mongo_date:
            sheet_date = self.parse_date(sheet_info['practice'])
            if not sheet_date or mongo_date > sheet_date:
                print(f"✓ Would UPDATE to: {self.format_date_for_sheets(mongo_date)}")
            else:
                print(f"⊘ No update needed (sheet date is current)")
        
        print("Recent practice history:")
        history = self.get_student_practice_history(cleaned_phone, limit=5)
        for i, record in enumerate(history, 1):
            print(f"  {i}. {record['date']} - {record['content']}")
        
        print("=" * 60)


def update_sheets_from_mongo():
    """
    Update Google Sheets with latest practice dates from MongoDB
    
    Returns:
        Dict with update statistics
    """
    try:
        updater = SheetsUpdater()
        results = updater.update_practice_dates()
        return results
    except Exception as e:
        print(f"Error updating sheets: {e}")
        import traceback
        traceback.print_exc()
        return None


# Example usage
if __name__ == "__main__":
    print("=" * 60)
    print("Google Sheets Practice Date Updater")
    print("=" * 60)
    
    update_sheets_from_mongo()