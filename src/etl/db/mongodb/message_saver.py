import hashlib
from datetime import datetime
from typing import List, Dict, Optional
from pymongo.errors import DuplicateKeyError
from src.etl.db.mongodb.mongo_handler import get_mongo_connection


class MessageSaver:

    def __init__(self):
        """Initialize connection and setup unique index"""
        self.mongo = get_mongo_connection()
        self.collection = self.mongo.get_students_messages_collection()
        self._setup_unique_index()
    
    def _setup_unique_index(self):
        """
        Create a unique index on message_hash to prevent duplicates
        
        The message_hash is created from: phone_number + timestamp + message_content
        This ensures the same message can't be inserted twice
        """
        try:
            # Create unique index on message_hash
            self.collection.create_index(
                "message_hash",
                unique=True,
                name="unique_message_idx"
            )
            print("✓ Unique index on message_hash created/verified")
        except Exception as e:
            print(f"Could not create unique index: {e}")
    
    @staticmethod
    def generate_message_hash(phone_number: str, timestamp: str, content: str) -> str:
        """
        Generate a unique hash for a message
        
        Args:
            phone_number: Phone number of sender/receiver
            timestamp: Message timestamp
            content: Message content/text
            
        Returns:
            str: SHA256 hash of the message (first 32 characters)
        """
        # Combine fields that make a message unique
        unique_string = f"{phone_number}|{timestamp}|{content}"
        
        # Create hash
        hash_object = hashlib.sha256(unique_string.encode())
        return hash_object.hexdigest()[:32]  # Use first 32 chars
    
    def save_message(self, message_data: Dict) -> Dict:
        """
        Save a single message to MongoDB
        
        Args:
            message_data: Dictionary containing message fields:
                - phone_number (required)
                - timestamp (required)
                - content (required)
                - name (optional)
                - lesson (optional)
                - Any other custom fields
        
        Returns:
            Dict with keys:
                - success: bool
                - action: 'inserted' or 'skipped'
                - message: str
                - message_id: str (if inserted)
        """
        try:
            # Validate required fields
            required_fields = ['phone_number', 'timestamp', 'content']
            missing_fields = [f for f in required_fields if f not in message_data]
            
            if missing_fields:
                return {
                    'success': False,
                    'action': 'error',
                    'message': f"Missing required fields: {missing_fields}"
                }
            
            # Generate unique hash
            message_hash = self.generate_message_hash(
                message_data['phone_number'],
                message_data['timestamp'],
                message_data['content']
            )
            
            # Add hash and metadata to message
            message_doc = {
                **message_data,
                'message_hash': message_hash,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            }
            
            # Try to insert
            result = self.collection.insert_one(message_doc)
            
            return {
                'success': True,
                'action': 'inserted',
                'message': 'Message saved successfully',
                'message_id': str(result.inserted_id)
            }
            
        except DuplicateKeyError:
            # Message already exists - this is expected for duplicates
            return {
                'success': True,
                'action': 'skipped',
                'message': 'Message already exists (duplicate)',
                'message_hash': message_hash
            }
            
        except Exception as e:
            return {
                'success': False,
                'action': 'error',
                'message': f"Error saving message: {str(e)}"
            }
    
    def save_messages_batch(self, messages: List[Dict]) -> Dict:
        """
        Save multiple messages in batch
        
        Args:
            messages: List of message dictionaries
            
        Returns:
            Dict with summary:
                - total: int (total messages processed)
                - inserted: int (new messages)
                - skipped: int (duplicates)
                - errors: int (failed)
                - details: List[Dict] (individual results)
        """
        results = {
            'total': len(messages),
            'inserted': 0,
            'skipped': 0,
            'errors': 0,
            'details': []
        }
        
        for message in messages:
            result = self.save_message(message)
            results['details'].append(result)
            
            if result['action'] == 'inserted':
                results['inserted'] += 1
            elif result['action'] == 'skipped':
                results['skipped'] += 1
            elif result['action'] == 'error':
                results['errors'] += 1
        
        return results
    
    def update_message(self, message_hash: str, update_data: Dict) -> Dict:
        """
        Update an existing message by its hash
        
        Args:
            message_hash: The unique hash of the message
            update_data: Dictionary of fields to update
            
        Returns:
            Dict with update result
        """
        try:
            # Add updated timestamp
            update_data['updated_at'] = datetime.utcnow()
            
            result = self.collection.update_one(
                {'message_hash': message_hash},
                {'$set': update_data}
            )
            
            if result.matched_count > 0:
                return {
                    'success': True,
                    'message': 'Message updated successfully',
                    'modified': result.modified_count > 0
                }
            else:
                return {
                    'success': False,
                    'message': 'Message not found'
                }
                
        except Exception as e:
            return {
                'success': False,
                'message': f"Error updating message: {str(e)}"
            }
    
    def get_messages_by_phone(self, phone_number: str, limit: Optional[int] = None) -> List[Dict]:
        """
        Get all messages for a phone number
        
        Args:
            phone_number: Phone number to search
            limit: Maximum number of messages to return (optional)
            
        Returns:
            List of message documents
        """
        query = {'phone_number': phone_number}
        cursor = self.collection.find(query).sort('timestamp', -1)  # Most recent first
        
        if limit:
            cursor = cursor.limit(limit)
        
        return list(cursor)
    
    def get_messages_by_lesson(self, lesson: int) -> List[Dict]:
        """
        Get all messages for a specific lesson
        
        Args:
            lesson: Lesson number
            
        Returns:
            List of message documents
        """
        return list(self.collection.find({'lesson': lesson}).sort('timestamp', -1))
    
    def get_message_count(self, phone_number: Optional[str] = None) -> int:
        """
        Get count of messages
        
        Args:
            phone_number: Optional phone number to filter by
            
        Returns:
            int: Number of messages
        """
        query = {'phone_number': phone_number} if phone_number else {}
        return self.collection.count_documents(query)
    
    def delete_duplicates_manual(self) -> Dict:
        """
        Manually scan and remove duplicate messages
        (Shouldn't be needed if unique index is working, but useful for cleanup)
        
        Returns:
            Dict with cleanup results
        """
        pipeline = [
            {
                '$group': {
                    '_id': '$message_hash',
                    'count': {'$sum': 1},
                    'ids': {'$push': '$_id'}
                }
            },
            {
                '$match': {
                    'count': {'$gt': 1}
                }
            }
        ]
        
        duplicates = list(self.collection.aggregate(pipeline))
        deleted_count = 0
        
        for dup in duplicates:
            # Keep the first, delete the rest
            ids_to_delete = dup['ids'][1:]
            result = self.collection.delete_many({'_id': {'$in': ids_to_delete}})
            deleted_count += result.deleted_count
        
        return {
            'duplicate_groups_found': len(duplicates),
            'documents_deleted': deleted_count
        }