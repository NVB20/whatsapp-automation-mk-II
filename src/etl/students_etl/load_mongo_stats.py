import hashlib
from datetime import datetime
from typing import List, Dict, Any
from collections import defaultdict

from src.etl.db.mongodb.mongo_handler import get_mongo_connection


def parse_timestamp(timestamp_str: str) -> datetime:
    """
    Parse timestamp string in format 'HH:MM, DD.MM.YYYY' to datetime object.
    """
    try:
        return datetime.strptime(timestamp_str, '%H:%M, %d.%m.%Y')
    except Exception as e:
        print(f"Error parsing timestamp '{timestamp_str}': {e}")
        raise


def generate_uniq_id(phone_number: str, name: str) -> str:
    """Generate a unique ID by hashing phone number and name."""
    combined = f"{phone_number}_{name}"
    return hashlib.md5(combined.encode()).hexdigest()


def aggregate_student_updates(transformed_records: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Group transformed records by student (phone_number).
    This allows us to process all messages for a student together.
    
    Returns: Dict with phone_number as key and list of messages as value
    """
    student_messages = defaultdict(list)
    
    for record in transformed_records:
        phone_number = record['phone_number']
        student_messages[phone_number].append(record)
    
    return student_messages


def process_student_messages(student_messages: List[Dict[str, Any]], stats_collection) -> Dict[str, Any]:
    """
    Process all messages for a single student and build the update operations.
    """
    # Get student info from first message (all messages have same student info)
    first_msg = student_messages[0]
    phone_number = first_msg['phone_number']
    name = first_msg['name']
    current_lesson = first_msg['lesson']
    uniq_id = generate_uniq_id(phone_number, name)
    
    # Fetch existing student document
    existing_doc = stats_collection.find_one({'uniq_id': uniq_id})
    
    # Initialize counters and tracking
    message_count_increment = 0
    last_message_timedate = None
    last_practice_timedate = None
    
    # Get existing timestamps from MongoDB to check for duplicates
    existing_last_message = None
    existing_last_practice = None
    
    if existing_doc:
        if existing_doc.get('last_message_timedate'):
            existing_last_message = parse_timestamp(existing_doc['last_message_timedate']) if isinstance(existing_doc['last_message_timedate'], str) else existing_doc['last_message_timedate']
        if existing_doc.get('last_practice_timedate'):
            existing_last_practice = parse_timestamp(existing_doc['last_practice_timedate']) if isinstance(existing_doc['last_practice_timedate'], str) else existing_doc['last_practice_timedate']
    
    # Get existing lessons or initialize empty
    existing_lessons = existing_doc.get('lessons', []) if existing_doc else []
    lessons_dict = {}
    for lesson in existing_lessons:
        lesson_copy = lesson.copy()
        # Parse timestamp strings to datetime objects for comparison
        if 'first_practice' in lesson_copy and isinstance(lesson_copy['first_practice'], str):
            lesson_copy['first_practice'] = parse_timestamp(lesson_copy['first_practice'])
        if 'last_practice' in lesson_copy and isinstance(lesson_copy['last_practice'], str):
            lesson_copy['last_practice'] = parse_timestamp(lesson_copy['last_practice'])
        lessons_dict[lesson['lesson']] = lesson_copy
    
    # Process each message
    for msg in student_messages:
        msg_type = msg['message_type']
        current_timestamp_str = msg['current_timestamp']
        current_timestamp = parse_timestamp(current_timestamp_str)
        msg_lesson = msg['lesson']
        msg_teacher = msg['teacher']
        
        if msg_type == 'message':
            # Check if this is a duplicate message (same or older timestamp)
            if existing_last_message and current_timestamp <= existing_last_message:
                print(f"  ⚠ Skipping duplicate message for {name} - timestamp {current_timestamp} <= existing {existing_last_message}")
                continue
            
            # Check against messages already processed in this batch
            if last_message_timedate and current_timestamp <= last_message_timedate:
                print(f"  ⚠ Skipping duplicate message in batch for {name}")
                continue
            
            # Increment message counter
            message_count_increment += 1
            
            # Update last_message_timedate to the most recent
            if last_message_timedate is None or current_timestamp > last_message_timedate:
                last_message_timedate = current_timestamp
        
        elif msg_type == 'practice':
            # Check if this is a duplicate practice (same or older timestamp)
            if existing_last_practice and current_timestamp <= existing_last_practice:
                print(f"  ⚠ Skipping duplicate practice for {name} - timestamp {current_timestamp} <= existing {existing_last_practice}")
                continue
            
            # Check against practices already processed in this batch
            if last_practice_timedate and current_timestamp <= last_practice_timedate:
                print(f"  ⚠ Skipping duplicate practice in batch for {name}")
                continue
            
            # Check if this lesson already exists
            if msg_lesson in lessons_dict:
                # Update existing lesson
                lesson_entry = lessons_dict[msg_lesson]
                
                # Only update if this practice is newer than the last one for this lesson
                if current_timestamp > lesson_entry['last_practice']:
                    lesson_entry['practice_count'] += 1
                    lesson_entry['teacher'] = msg_teacher
                    lesson_entry['last_practice'] = current_timestamp
                    
                    # Update the global last_practice_timedate
                    if last_practice_timedate is None or current_timestamp > last_practice_timedate:
                        last_practice_timedate = current_timestamp
                else:
                    print(f"  ⚠ Skipping duplicate practice for {name} lesson {msg_lesson} - already processed")
                    continue
            else:
                # Create new lesson entry
                new_lesson = {
                    'lesson': msg_lesson,
                    'teacher': msg_teacher,
                    'practice_count': 1,
                    'first_practice': current_timestamp,
                    'last_practice': current_timestamp
                }
                lessons_dict[msg_lesson] = new_lesson
                
                # Update the global last_practice_timedate for new lessons
                if last_practice_timedate is None or current_timestamp > last_practice_timedate:
                    last_practice_timedate = current_timestamp
    
    # Convert lessons_dict back to list
    lessons_list = list(lessons_dict.values())
    
    # Handle increments and updates
    set_operations = {}
    inc_operations = {}
    
    if message_count_increment > 0:
        inc_operations['total_messages'] = message_count_increment
    
    # Convert datetime objects back to strings for MongoDB storage
    if last_message_timedate:
        set_operations['last_message_timedate'] = last_message_timedate.strftime('%H:%M, %d.%m.%Y')
    
    if last_practice_timedate:
        set_operations['last_practice_timedate'] = last_practice_timedate.strftime('%H:%M, %d.%m.%Y')
    
    # Convert lesson timestamps back to strings
    for lesson in lessons_list:
        if 'first_practice' in lesson and isinstance(lesson['first_practice'], datetime):
            lesson['first_practice'] = lesson['first_practice'].strftime('%H:%M, %d.%m.%Y')
        if 'last_practice' in lesson and isinstance(lesson['last_practice'], datetime):
            lesson['last_practice'] = lesson['last_practice'].strftime('%H:%M, %d.%m.%Y')
    
    # Always update lessons array
    set_operations['lessons'] = lessons_list
    
    # Add other fields to set
    set_operations.update({
        'phone_number': phone_number,
        'name': name,
        'current_lesson': current_lesson,
        'updated_at': datetime.now()
    })
    
    # If new document, set created_at and initialize total_messages
    if not existing_doc:
        set_operations['created_at'] = datetime.now()
        set_operations['total_messages'] = message_count_increment
        inc_operations = {}  # Don't increment on new docs, just set
    
    return {
        'filter': {'uniq_id': uniq_id},
        'update': {
            '$set': set_operations,
            **({'$inc': inc_operations} if inc_operations else {})
        },
        'upsert': True,
        'is_new': not existing_doc  # Track if this is a new document
    }


def load(transformed_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Load transformed records into MongoDB student_stats collection.
    """
    if not transformed_records:
        print("No records to load")
        return {
            'students_processed': 0,
            'new_students': 0,
            'updated_students': 0,
            'messages_loaded': 0,
            'practices_loaded': 0,
            'errors': 0
        }
    
    print(f"\n{'='*60}")
    print(f"Starting load for {len(transformed_records)} records")
    print(f"{'='*60}")
    
    # Get MongoDB connection
    mongo_conn = get_mongo_connection()
    stats_collection = mongo_conn.get_students_stats_collection()
    
    # Aggregate messages by student
    student_messages_map = aggregate_student_updates(transformed_records)
    
    print(f"Processing {len(student_messages_map)} students")
    
    # Statistics
    stats = {
        'students_processed': 0,
        'new_students': 0,
        'updated_students': 0,
        'messages_loaded': 0,
        'practices_loaded': 0,
        'errors': 0
    }
    
    # Process each student
    for phone_number, student_messages in student_messages_map.items():
        try:
            # Count message types
            message_count = sum(1 for msg in student_messages if msg['message_type'] == 'message')
            practice_count = sum(1 for msg in student_messages if msg['message_type'] == 'practice')
            
            # Process all messages for this student
            update_operation = process_student_messages(student_messages, stats_collection)
            
            # Execute MongoDB update
            result = stats_collection.update_one(
                update_operation['filter'],
                update_operation['update'],
                upsert=update_operation['upsert']
            )
            
            # Update statistics
            stats['students_processed'] += 1
            
            # Track new vs updated based on upserted_id presence
            if result.upserted_id or update_operation['is_new']:
                stats['new_students'] += 1
            else:
                stats['updated_students'] += 1
            
            stats['messages_loaded'] += message_count
            stats['practices_loaded'] += practice_count
            
            student_name = student_messages[0]['name']
            status = "NEW" if (result.upserted_id or update_operation['is_new']) else "UPDATED"
            print(f"✓ {status}: {student_name} ({phone_number}) - {message_count} messages, {practice_count} practices")
            
        except Exception as e:
            stats['errors'] += 1
            print(f"✗ Error processing {phone_number}: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n{'='*60}")
    print(f"Load complete:")
    print(f"  Students processed: {stats['students_processed']}")
    print(f"  New students: {stats['new_students']}")
    print(f"  Updated students: {stats['updated_students']}")
    print(f"  Messages loaded: {stats['messages_loaded']}")
    print(f"  Practices loaded: {stats['practices_loaded']}")
    print(f"  Errors: {stats['errors']}")
    print(f"{'='*60}\n")
    
    return stats