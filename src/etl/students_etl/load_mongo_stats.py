import hashlib
from datetime import datetime
from typing import List, Dict, Any
from collections import defaultdict

from src.etl.db.mongodb.mongo_handler import get_mongo_connection, MongoDBConnection


def parse_timestamp(timestamp_str: str) -> datetime:
    """
    Parse timestamp string in multiple formats to datetime object.
    Supports:
    - ISO 8601: '2025-12-02T16:15:42.998+00:00'
    - Custom format: 'HH:MM, DD.MM.YYYY'
    """
    try:
        # Try ISO 8601 format first
        if 'T' in timestamp_str:
            # Handle ISO format with timezone
            return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        else:
            # Handle custom format
            return datetime.strptime(timestamp_str, '%H:%M, %d.%m.%Y')
    except Exception as e:
        print(f"Error parsing timestamp '{timestamp_str}': {e}")
        raise


def format_timestamp(dt: datetime) -> str:
    """
    Format datetime object to string in format 'HH:MM, DD.MM.YYYY'
    Uses zero-padded format for consistency
    """
    return dt.strftime('%H:%M, %d.%m.%Y')


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
    Process all messages for a single student and build MongoDB update operations.
    Clean, predictable, auto-advancing lessons, duplicate-safe.
    """
    first_msg = student_messages[0]
    phone_number = first_msg['phone_number']
    name = first_msg['name']
    current_lesson = first_msg['lesson']
    uniq_id = generate_uniq_id(phone_number, name)

    # Fetch existing student
    existing_doc = stats_collection.find_one({'uniq_id': uniq_id})

    existing_last_message = None
    existing_last_practice = None

    if existing_doc:
        if existing_doc.get('last_message_timedate'):
            val = existing_doc['last_message_timedate']
            existing_last_message = parse_timestamp(val) if isinstance(val, str) else val

        if existing_doc.get('last_practice_timedate'):
            val = existing_doc['last_practice_timedate']
            existing_last_practice = parse_timestamp(val) if isinstance(val, str) else val

    # Load lessons into dict
    lessons_dict = {}
    if existing_doc and 'lessons' in existing_doc:
        for lesson in existing_doc['lessons']:
            if not isinstance(lesson, dict):
                print(f"⚠ Corrupt lesson structure in DB for {name}, skipping: {lesson}")
                continue

            lesson_copy = lesson.copy()

            if 'lesson' not in lesson_copy:
                print(f"⚠ Lesson entry missing 'lesson' key for {name}, skipping: {lesson_copy}")
                continue

            # Parse timestamps
            for key in ['first_practice', 'last_practice']:
                if key in lesson_copy and isinstance(lesson_copy[key], str):
                    try:
                        lesson_copy[key] = parse_timestamp(lesson_copy[key])
                    except:
                        pass

            # Ensure practice_count is an integer
            if 'practice_count' in lesson_copy:
                try:
                    lesson_copy['practice_count'] = int(lesson_copy['practice_count'])
                except (ValueError, TypeError):
                    lesson_copy['practice_count'] = 0

            # Ensure message_count exists (backward compatibility)
            if 'message_count' not in lesson_copy:
                lesson_copy['message_count'] = 0
            else:
                try:
                    lesson_copy['message_count'] = int(lesson_copy['message_count'])
                except (ValueError, TypeError):
                    lesson_copy['message_count'] = 0

            # Ensure paid exists (backward compatibility - default to False)
            if 'paid' not in lesson_copy:
                lesson_copy['paid'] = False
            else:
                # Ensure it's a boolean
                lesson_copy['paid'] = bool(lesson_copy['paid'])

            lessons_dict[lesson_copy['lesson']] = lesson_copy

    last_message_timedate = None
    last_practice_timedate = None

    # PROCESS INPUT MESSAGES
    for msg in student_messages:
        msg_type = msg['message_type']
        ts = parse_timestamp(msg['current_timestamp'])
        msg_lesson = msg['lesson']
        msg_teacher = msg['teacher']

        if msg_type == 'message':
            if existing_last_message and ts <= existing_last_message:
                continue

            if last_message_timedate and ts <= last_message_timedate:
                continue

            # Increment message count at lesson level
            if msg_lesson in lessons_dict:
                lesson_entry = lessons_dict[msg_lesson]
                lesson_entry['message_count'] = lesson_entry.get('message_count', 0) + 1
            else:
                # Create lesson entry if it doesn't exist
                lessons_dict[msg_lesson] = {
                    'lesson': msg_lesson,
                    'teacher': msg_teacher,
                    'practice_count': 0,
                    'message_count': 1,
                    'first_practice': None,
                    'last_practice': None,
                    'paid': False
                }

            if not last_message_timedate or ts > last_message_timedate:
                last_message_timedate = ts

        elif msg_type == 'practice':
            if existing_last_practice and ts <= existing_last_practice:
                continue

            if last_practice_timedate and ts <= last_practice_timedate:
                continue

            if msg_lesson in lessons_dict:
                lesson_entry = lessons_dict[msg_lesson]

                if ts > lesson_entry.get('last_practice', datetime.min):
                    lesson_entry['practice_count'] = lesson_entry.get('practice_count', 0) + 1
                    lesson_entry['teacher'] = msg_teacher
                    lesson_entry['last_practice'] = ts

                    if not lesson_entry.get('first_practice'):
                        lesson_entry['first_practice'] = ts

                    if not last_practice_timedate or ts > last_practice_timedate:
                        last_practice_timedate = ts
                else:
                    continue
            else:
                lessons_dict[msg_lesson] = {
                    'lesson': msg_lesson,
                    'teacher': msg_teacher,
                    'practice_count': 1,
                    'message_count': 0,
                    'first_practice': ts,
                    'last_practice': ts,
                    'paid': False
                }

                if not last_practice_timedate or ts > last_practice_timedate:
                    last_practice_timedate = ts

    # AUTO-ADD CURRENT LESSON
    if current_lesson not in lessons_dict:
        lessons_dict[current_lesson] = {
            'lesson': current_lesson,
            'teacher': first_msg['teacher'],
            'practice_count': 0,
            'message_count': 0,
            'first_practice': None,
            'last_practice': None,
            'paid': False
        }

    # SORT LESSONS
    def extract_number(lesson_name):
        try:
            return int(''.join(filter(str.isdigit, lesson_name)))
        except:
            return 9999

    lessons_list = sorted(lessons_dict.values(), key=lambda x: extract_number(x['lesson']))

    # --- CLEAN LESSONS BEFORE SAVING (convert datetimes to strings) ---
    for lesson in lessons_list:
        for key in ['first_practice', 'last_practice']:
            if isinstance(lesson.get(key), datetime):
                lesson[key] = format_timestamp(lesson[key])

    # PREPARE SETS FOR MONGO - USE STRING TIMESTAMPS
    set_ops = {
        'phone_number': phone_number,
        'name': name,
        'current_lesson': current_lesson,
        'lessons': lessons_list,
        'updated_at': MongoDBConnection.get_current_timestamp(),
    }

    if last_message_timedate:
        set_ops['last_message_timedate'] = format_timestamp(last_message_timedate)

    if last_practice_timedate:
        set_ops['last_practice_timedate'] = format_timestamp(last_practice_timedate)

    if not existing_doc:
        set_ops['created_at'] = MongoDBConnection.get_current_timestamp()

    update_doc = {'$set': set_ops}

    # Remove deprecated total_messages field if it exists
    update_doc['$unset'] = {'total_messages': ""}

    return {
        'filter': {'uniq_id': uniq_id},
        'update': update_doc,
        'upsert': True,
        'is_new': not existing_doc
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


def migrate_existing_data():
    """
    Migration helper function to add new fields to existing lesson records.

    This function:
    1. Adds 'paid' field (default: False) to lessons missing it
    2. Adds 'message_count' field (default: 0) to lessons missing it
    3. Preserves all existing data

    Run this once after deploying the new code to migrate existing records.
    Safe to run multiple times - it only updates missing fields.
    """
    print(f"\n{'='*60}")
    print("MIGRATION: Adding new fields to existing lesson records")
    print(f"{'='*60}\n")

    mongo_conn = get_mongo_connection()
    stats_collection = mongo_conn.get_students_stats_collection()

    # Find all student documents
    all_students = stats_collection.find({})

    migrated_count = 0
    error_count = 0

    for student in all_students:
        try:
            uniq_id = student.get('uniq_id')
            name = student.get('name', 'Unknown')
            lessons = student.get('lessons', [])

            if not lessons:
                continue

            updated_lessons = []
            needs_update = False

            for lesson in lessons:
                if not isinstance(lesson, dict):
                    updated_lessons.append(lesson)
                    continue

                lesson_copy = lesson.copy()

                # Add paid field if missing
                if 'paid' not in lesson_copy:
                    lesson_copy['paid'] = False
                    needs_update = True

                # Add message_count field if missing
                if 'message_count' not in lesson_copy:
                    lesson_copy['message_count'] = 0
                    needs_update = True

                updated_lessons.append(lesson_copy)

            # Update document if any lessons were modified
            if needs_update:
                stats_collection.update_one(
                    {'uniq_id': uniq_id},
                    {
                        '$set': {
                            'lessons': updated_lessons,
                            'updated_at': MongoDBConnection.get_current_timestamp()
                        }
                    }
                )
                migrated_count += 1
                print(f"✓ Migrated: {name} ({len(updated_lessons)} lessons)")

        except Exception as e:
            error_count += 1
            print(f"✗ Error migrating student {student.get('name', 'Unknown')}: {e}")
            import traceback
            traceback.print_exc()

    print(f"\n{'='*60}")
    print(f"Migration complete:")
    print(f"  Students migrated: {migrated_count}")
    print(f"  Errors: {error_count}")
    print(f"{'='*60}\n")

    return {'migrated': migrated_count, 'errors': error_count}


if __name__ == '__main__':
    """
    Run migration to add new fields to existing data.
    Usage: python -m src.etl.students_etl.load_mongo_stats
    """
    print("Running migration to add 'paid' and 'message_count' fields to lessons...")
    migrate_existing_data()