import os
import platform
import subprocess
import json
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

# Load environment variables
load_dotenv()

# MongoDB configuration
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_CONTAINER_NAME = os.getenv("MONGO_CONTAINER_NAME", "mongodb")  # Default container name

MONGO_USERNAME = os.getenv("MONGO_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")

MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")


def is_windows():
    return platform.system().lower() == 'windows'


def is_wsl():
    try:
        with open('/proc/version', 'r') as f:
            return 'microsoft' in f.read().lower()
    except:
        return False


def get_docker_container_ip(container_name):
    try:
        # Method 1: Using docker inspect
        result = subprocess.run(
            ['docker', 'inspect', '-f', '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}', container_name],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode == 0 and result.stdout.strip():
            ip = result.stdout.strip()
            print(f"✓ Found container '{container_name}' at IP: {ip}")
            return ip
            
    except subprocess.TimeoutExpired:
        print(f"⚠️ Docker command timed out")
    except FileNotFoundError:
        print(f"⚠️ Docker command not found. Is Docker installed?")
    except Exception as e:
        print(f"⚠️ Error getting container IP: {e}")
    
    return None


def get_mongo_host():  
    # On Windows or WSL, Docker container IPs are not accessible from host
    # Always use localhost with port mapping
    if is_windows():
        print(f"Detected Windows - using localhost (container IPs not accessible from Windows host)")
        return "localhost"
    
    if is_wsl():
        print(f"Detected WSL - using localhost (container IPs not accessible from WSL)")
        return "localhost"
    
    # On Linux, we can try to use container IP
    container_name = os.getenv("MONGO_CONTAINER_NAME")
    if container_name:
        print(f"Detected Linux - attempting to find Docker container: {container_name}")
        container_ip = get_docker_container_ip(container_name)
        if container_ip:
            print(f"Note: Using container IP. If this fails, set MONGO_HOST=localhost in .env")
            return container_ip
    
    # Fallback to localhost
    print(f"Using default: localhost")
    return "localhost"


def list_mongo_containers():
    try:
        # Get all running containers
        result = subprocess.run(
            ['docker', 'ps', '--format', '{{json .}}'],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode == 0:
            containers = []
            for line in result.stdout.strip().split('\n'):
                if line:
                    container = json.loads(line)
                    # Check if it's a MongoDB container
                    if 'mongo' in container.get('Image', '').lower() or 'mongo' in container.get('Names', '').lower():
                        containers.append({
                            'name': container.get('Names'),
                            'id': container.get('ID'),
                            'image': container.get('Image'),
                            'ports': container.get('Ports')
                        })
            
            if containers:
                print(f"\nFound {len(containers)} MongoDB container(s):")
                for c in containers:
                    print(f"   - {c['name']} (ID: {c['id'][:12]}, Image: {c['image']})")
            
            return containers
    except Exception as e:
        print(f"Error listing containers: {e}")
    
    return []


def build_mongo_uri(host):
    """Build MongoDB connection URI"""
    if MONGO_USERNAME and MONGO_PASSWORD:
        # With authentication
        return f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{host}:{MONGO_PORT}/"
    else:
        # Without authentication
        return f"mongodb://{host}:{MONGO_PORT}/"


class MongoDBConnection:
    """Handler for MongoDB connection and setup"""
    
    _instance = None
    _client = None
    _db = None
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
    
    def _connect(self):
        """Establish connection to MongoDB"""
        # Auto-detect MongoDB host
        self._host = get_mongo_host()
        mongo_uri = build_mongo_uri(self._host)
        
        try:
            print(f"\nAttempting to connect to MongoDB...")
            print(f"   Host: {self._host}:{MONGO_PORT}")
            print(f"   Database: {MONGO_DB_NAME}")
            
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
            
            self._db = self._client[MONGO_DB_NAME]
            print(f"✓ Using database: {MONGO_DB_NAME}")
            
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
        """Setup collections and create indexes"""
        try:
            # Get collection references
            messages_collection = self._db[COLLECTION_NAME]
            
            print(f"\nSetting up collections...")
            
            # Create indexes for messages collection
            self._create_indexes(messages_collection, COLLECTION_NAME)
            
            print(f"✓ Collections and indexes setup complete")
            
        except Exception as e:
            print(f"⚠️ Warning: Could not setup collections: {e}")
    
    def _create_indexes(self, collection, collection_name):
        """Create indexes for a collection"""
        try:
            # Index on phone_number for fast lookups
            collection.create_index([("phone_number", ASCENDING)], name="phone_number_idx")
            
            # Index on timestamp for sorting
            collection.create_index([("timestamp", ASCENDING)], name="timestamp_idx")
            
            # Compound index for phone + timestamp
            collection.create_index([
                ("phone_number", ASCENDING),
                ("timestamp", ASCENDING)
            ], name="phone_timestamp_idx")
            
            # Index on lesson for filtering
            collection.create_index([("lesson", ASCENDING)], name="lesson_idx")
            
            # Index on name for searching
            collection.create_index([("name", ASCENDING)], name="name_idx")
            
            print(f"  ✓ Created indexes for {collection_name}")
            
        except Exception as e:
            print(f"  ⚠️ Could not create indexes for {collection_name}: {e}")
    
    def get_database(self):
        """Get MongoDB database instance"""
        if self._db is None:
            self._connect()
        return self._db
    
    def get_collection(self, collection_name):
        """Get a specific collection"""
        return self.get_database()[collection_name]
    
    def get_messages_collection(self):
        """Get sent messages collection"""
        return self.get_collection(COLLECTION_NAME)
    
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
            "database": MONGO_DB_NAME,
            "messages_collection": COLLECTION_NAME,
            "is_connected": self.test_connection()
        }
    
    def list_collections(self):
        """List all collections in the database"""
        try:
            collections = self._db.list_collection_names()
            print(f"\nCollections in {MONGO_DB_NAME}:")
            for col in collections:
                count = self._db[col].count_documents({})
                print(f"   - {col}: {count} documents")
            return collections
        except Exception as e:
            print(f"Error listing collections: {e}")
            return []
    
    def close(self):
        """Close MongoDB connection"""
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
            print("\n🔌 Closed MongoDB connection")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


def get_mongo_connection():
    """Get MongoDB connection instance"""
    return MongoDBConnection()
