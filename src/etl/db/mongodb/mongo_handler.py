import os
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from src.etl.db.mongodb.mongo_finder import get_mongo_host, build_mongo_uri, list_mongo_containers

# Only load .env if environment variables aren't already set (docker-compose takes precedence)
if not os.getenv('MONGO_HOST'):
    load_dotenv()

# MongoDB configuration
MONGO_PORT = os.getenv("MONGO_PORT")
MONGO_CONTAINER_NAME = os.getenv("MONGO_CONTAINER_NAME")

MONGO_USERNAME = os.getenv("MONGO_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")

# Students database configuration
STUDENTS_DB = os.getenv("STUDENTS_DB")
STUDENTS_STATS = os.getenv("STUDENTS_STATS")

# Sales database configuration
SALES_DB = os.getenv("SALES_DB")
SALES_LAST_RUN_COLLECTION = os.getenv("SALES_LAST_RUN_COLLECTION")

# Logger database configuration
LOGGER_DB = os.getenv("LOGGER_DB")
LOGGER_STATS = os.getenv("LOGGER_STATS")

# Validate required environment variables
required_vars = {
    "STUDENTS_DB": STUDENTS_DB,
    "STUDENTS_STATS": STUDENTS_STATS,
    "SALES_DB": SALES_DB,
    "SALES_LAST_RUN_COLLECTION": SALES_LAST_RUN_COLLECTION,
    "LOGGER_DB": LOGGER_DB,
    "LOGGER_STATS": LOGGER_STATS
}

missing_vars = [var for var, value in required_vars.items() if not value]
if missing_vars:
    error_msg = f"""
{'='*60}
CONFIGURATION ERROR - Missing Environment Variables
{'='*60}
The following required environment variables are not set:
{chr(10).join(f'  - {var}' for var in missing_vars)}

Please check your .env file and ensure it contains:
  STUDENTS_DB=students_db
  STUDENTS_STATS=student_stats
  SALES_DB=sales_db
  SALES_LAST_RUN_COLLECTION=last_run_timestamp
  LOGGER_DB=logger_db
  LOGGER_STATS=logger_stats

Current .env location: {os.path.abspath('.env')}
{'='*60}
"""
    raise ValueError(error_msg)

class MongoDBConnection:
    """Handler for MongoDB connection and setup"""
    
    _instance = None
    _client = None
    _students_db = None
    _sales_db = None
    _logger_db = None
    _host = None
    
    def __new__(cls):
        """Singleton pattern to ensure single connection"""
        if cls._instance is None:
            cls._instance = super(MongoDBConnection, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize MongoDB connection if not already connected"""
        if self._client is None:
            self._connect()
    
    @staticmethod
    def get_current_timestamp():
        """
        Get current timestamp as string in format: HH:MM, DD.MM.YYYY
        Returns: String timestamp (e.g., "14:30, 09.12.2025")
        """
        now = datetime.now()
        return now.strftime("%H:%M, %d.%m.%Y")
    
    @staticmethod
    def parse_timestamp(timestamp_str):
        """
        Parse timestamp string back to datetime object
        Args:
            timestamp_str: String in format "HH:MM, DD.MM.YYYY"
        Returns: datetime object or None if parsing fails
        """
        try:
            return datetime.strptime(timestamp_str, "%H:%M, %d.%m.%Y")
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def add_timestamps(document, include_created=True, include_updated=True):
        """
        Add timestamp fields to a document
        Args:
            document: Dictionary to add timestamps to
            include_created: Whether to add created_at field
            include_updated: Whether to add updated_at field
        Returns: Modified document with timestamps
        """
        current_time = MongoDBConnection.get_current_timestamp()
        
        if include_created and 'created_at' not in document:
            document['created_at'] = current_time
        
        if include_updated:
            document['updated_at'] = current_time
        
        return document
    
    def _connect(self):
        """Establish connection to MongoDB"""
        # Auto-detect MongoDB host
        self._host = get_mongo_host()
        mongo_uri = build_mongo_uri(self._host)
        
        try:
            print(f"Attempting to connect to MongoDB...")
            print(f"   Host: {self._host}:{MONGO_PORT}")
            print(f"   Students Database: {STUDENTS_DB}")
            print(f"   Sales Database: {SALES_DB}")
            print(f"   Logger Database: {LOGGER_DB}")
            
            # Create client with timeout settings
            self._client = MongoClient(
                mongo_uri,
                serverSelectionTimeoutMS=5000,  # 5 second timeout
                connectTimeoutMS=5000,
                socketTimeoutMS=5000
            )
            
            # Test connection
            self._client.admin.command('ping')
            print(f"✓ Successfully connected to MongoDB!")
            
            # Setup all databases
            self._students_db = self._client[STUDENTS_DB]
            self._sales_db = self._client[SALES_DB]
            self._logger_db = self._client[LOGGER_DB]
            print(f"✓ Using databases: {STUDENTS_DB}, {SALES_DB}, {LOGGER_DB}")
            
            # Setup collections and indexes
            self._setup_collections()
            
        except ServerSelectionTimeoutError:
            print(f"Could not connect to MongoDB at {self._host}:{MONGO_PORT}")
            print(f"Trying to find MongoDB containers...")
            list_mongo_containers()
            raise
        except ConnectionFailure as e:
            print(f"Failed to connect to MongoDB: {e}")
            raise
        except Exception as e:
            print(f"Error during MongoDB setup: {e}")
            raise
    
    def _setup_collections(self):
        """
        Setup collections and create indexes
        Only creates these specific collections:
        - students_db.student_stats
        - sales_db.last_run_timestamp
        - logger_db.logger_stats
        """
        try:
            print(f"Setting up collections...")
            
            # Get collection references (exact names from .env)
            students_stats_collection = self._students_db[STUDENTS_STATS]
            sales_last_run_collection = self._sales_db[SALES_LAST_RUN_COLLECTION]
            logger_stats_collection = self._logger_db[LOGGER_STATS]
            
            # Create indexes for student_stats collection
            self._create_student_stats_indexes(students_stats_collection, STUDENTS_STATS)
            
            # Create indexes for sales last_run_timestamp collection
            self._create_last_run_indexes(sales_last_run_collection, SALES_LAST_RUN_COLLECTION)
            
            # Create indexes for logger_stats collection
            self._create_logger_stats_indexes(logger_stats_collection, LOGGER_STATS)
            
            print(f"✓ Collections and indexes setup complete")
            
        except Exception as e:
            print(f"Warning: Could not setup collections: {e}")
    
    def _create_student_stats_indexes(self, collection, collection_name):
        """
        Create indexes for student_stats collection

        Schema note: Each lesson object now contains:
        - lesson: str
        - teacher: str
        - practice_count: int
        - first_practice: str (timestamp)
        - last_practice: str (timestamp)
        - paid: bool (payment status for this class)
        - message_count: int (messages sent for this class)
        """
        try:
            # Index on phone_number for fast lookups
            collection.create_index([("phone_number", ASCENDING)], name="phone_number_idx")

            # Index on uniq_id (unique identifier)
            collection.create_index([("uniq_id", ASCENDING)], unique=True, name="uniq_id_idx")

            # Index on current_lesson for filtering
            collection.create_index([("current_lesson", ASCENDING)], name="current_lesson_idx")

            # Index on updated_at for sorting (now a string)
            collection.create_index([("updated_at", ASCENDING)], name="updated_at_idx")

            # Index on created_at for sorting (now a string)
            collection.create_index([("created_at", ASCENDING)], name="created_at_idx")

            # Index on name for searching
            collection.create_index([("name", ASCENDING)], name="name_idx")

            # Index on lessons.paid for payment status queries
            collection.create_index([("lessons.paid", ASCENDING)], name="lessons_paid_idx")

            print(f"   ✓ Created indexes for {collection_name} (student statistics)")

        except Exception as e:
            print(f"   ⚠ Could not create indexes for {collection_name}: {e}")
    
    def _create_last_run_indexes(self, collection, collection_name):
        """Create indexes for last_run_timestamp collection"""
        try:
            # Index on identifier (job name or process name)
            collection.create_index([("identifier", ASCENDING)], name="identifier_idx", unique=True)
            
            # Index on last_run_timestamp
            collection.create_index([("last_run_timestamp", ASCENDING)], name="last_run_timestamp_idx")
            
            print(f"   ✓ Created indexes for {collection_name} (tracking)")
            
        except Exception as e:
            print(f"   ⚠ Could not create indexes for {collection_name}: {e}")
    
    def _create_logger_stats_indexes(self, collection, collection_name):
        """Create indexes for logger_stats collection"""
        try:
            # Index on timestamp for chronological queries
            collection.create_index([("timestamp", ASCENDING)], name="timestamp_idx")
            
            # Index on log_level for filtering (info, warning, error, etc.)
            collection.create_index([("log_level", ASCENDING)], name="log_level_idx")
            
            # Index on source/module for filtering by origin
            collection.create_index([("source", ASCENDING)], name="source_idx")
            
            # Compound index for time-based queries by level
            collection.create_index([
                ("log_level", ASCENDING),
                ("timestamp", ASCENDING)
            ], name="level_timestamp_idx")
            
            # Compound index for source + timestamp queries
            collection.create_index([
                ("source", ASCENDING),
                ("timestamp", ASCENDING)
            ], name="source_timestamp_idx")
            
            print(f"   ✓ Created indexes for {collection_name} (logger statistics)")
            
        except Exception as e:
            print(f"   ⚠ Could not create indexes for {collection_name}: {e}")
    
    def get_students_database(self):
        """Get Students MongoDB database instance"""
        if self._students_db is None:
            self._connect()
        return self._students_db
    
    def get_sales_database(self):
        """Get Sales MongoDB database instance"""
        if self._sales_db is None:
            self._connect()
        return self._sales_db
    
    def get_logger_database(self):
        """Get Logger MongoDB database instance"""
        if self._logger_db is None:
            self._connect()
        return self._logger_db
    
    def get_collection(self, database_name, collection_name):
        """Get a specific collection from a database"""
        if database_name == "students":
            return self.get_students_database()[collection_name]
        elif database_name == "sales":
            return self.get_sales_database()[collection_name]
        elif database_name == "logger":
            return self.get_logger_database()[collection_name]
        else:
            raise ValueError(f"Unknown database: {database_name}")
    
    def get_students_stats_collection(self):
        """Get student_stats collection from students_db"""
        return self.get_students_database()[STUDENTS_STATS]
    
    def get_sales_last_run_collection(self):
        """Get last_run_timestamp collection from sales_db"""
        return self.get_sales_database()[SALES_LAST_RUN_COLLECTION]
    
    def get_logger_stats_collection(self):
        """Get logger_stats collection from logger_db"""
        return self.get_logger_database()[LOGGER_STATS]
    
    def insert_with_timestamps(self, collection, document):
        """
        Insert a document with automatic timestamps
        Args:
            collection: MongoDB collection object
            document: Document to insert
        Returns: Insert result
        """
        document = self.add_timestamps(document, include_created=True, include_updated=True)
        return collection.insert_one(document)
    
    def update_with_timestamp(self, collection, filter_query, update_data, upsert=False):
        """
        Update a document and automatically set updated_at timestamp
        Args:
            collection: MongoDB collection object
            filter_query: Query to find document
            update_data: Data to update (should be a dict, not $set operator)
            upsert: Whether to insert if not found
        Returns: Update result
        """
        # Add updated_at to the update data
        update_data['updated_at'] = self.get_current_timestamp()
        
        # If upserting, also add created_at
        if upsert:
            update_operation = {
                "$set": update_data,
                "$setOnInsert": {"created_at": self.get_current_timestamp()}
            }
        else:
            update_operation = {"$set": update_data}
        
        return collection.update_one(filter_query, update_operation, upsert=upsert)
    
    def test_connection(self):
        """Test if connection is alive"""
        try:
            self._client.admin.command('ping')
            return True
        except Exception:
            return False
    
    def get_connection_info(self):
        """Get connection information"""
        return {
            "host": self._host,
            "port": MONGO_PORT,
            "students_database": STUDENTS_DB,
            "students_stats_collection": STUDENTS_STATS,
            "sales_database": SALES_DB,
            "sales_last_run_collection": SALES_LAST_RUN_COLLECTION,
            "logger_database": LOGGER_DB,
            "logger_stats_collection": LOGGER_STATS,
            "is_connected": self.test_connection()
        }
    
    def list_collections(self):
        """List all collections in all databases"""
        try:
            print(f"\n📚 Collections in {STUDENTS_DB}:")
            students_collections = self._students_db.list_collection_names()
            for col in students_collections:
                count = self._students_db[col].count_documents({})
                print(f"   - {col}: {count} documents")
            
            print(f"\n💼 Collections in {SALES_DB}:")
            sales_collections = self._sales_db.list_collection_names()
            for col in sales_collections:
                count = self._sales_db[col].count_documents({})
                print(f"   - {col}: {count} documents")
            
            print(f"\n📝 Collections in {LOGGER_DB}:")
            logger_collections = self._logger_db.list_collection_names()
            for col in logger_collections:
                count = self._logger_db[col].count_documents({})
                print(f"   - {col}: {count} documents")
            
            return {
                "students_db": students_collections,
                "sales_db": sales_collections,
                "logger_db": logger_collections
            }
        except Exception as e:
            print(f"Error listing collections: {e}")
            return {}
    
    def close(self):
        """Close MongoDB connection"""
        if self._client:
            self._client.close()
            self._client = None
            self._students_db = None
            self._sales_db = None
            self._logger_db = None
            print("Closed MongoDB connection")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


def get_mongo_connection():
    """Get MongoDB connection instance"""
    return MongoDBConnection()