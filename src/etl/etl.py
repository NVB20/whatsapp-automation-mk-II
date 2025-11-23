from src.etl.extract import open_whatsapp
from src.etl.transform import process_messages
from src.etl.db.mongodb.mongo_connect import get_mongo_connection

def run_etl():
    messages = open_whatsapp()
    if messages == 0:
        print("failed to read messages")
    else:
        messages = process_messages(messages)
        print(messages)
        mongo = get_mongo_connection()
        db = mongo.get_database()    