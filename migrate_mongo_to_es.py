import json
from bson import ObjectId
from elasticsearch import Elasticsearch, helpers
from pymongo import MongoClient
from utils import get_config


def convert_to_valid_es_id(mongo_id):
    if isinstance(mongo_id, ObjectId):
        return str(mongo_id)
    return mongo_id


def migrate_mongo_to_es(es_config, mongo_config, collection_name, index_name, mapping):
    # Initialize clients
    es_client = Elasticsearch(hosts=es_config['hosts'], basic_auth=("elastic", "ybY9LVP=Jqmbx5lYgMGF"))
    # es_client = Elasticsearch(hosts=es_config['hosts'])
    mongo_client = MongoClient(mongo_config['uri'])
    db = mongo_client[mongo_config['database']]
    collection = db[collection_name]

    # Get all documents from MongoDB collection

    if not es_client.indices.exists(index=index_name):
        es_client.indices.create(index=index_name, body=mapping)
        print('creating index')

    # Get all documents from MongoDB collection
    documents = collection.find()

    # Iterate through documents and index them in Elasticsearch
    for document in documents:
        es_id = convert_to_valid_es_id(document['_id'])
        del document['_id']  # Remove MongoDB ID before indexing in Elasticsearch

        # Index document in Elasticsearch
        es_client.index(index=index_name, id=es_id, document=document)

    # Release resources
    es_client.close()
    mongo_client.close()


# Get configuration details
es_config, mongo_config = get_config()

# Specify collection name and index name
collection_name = 'shakespeare1'
index_name = 'shakespeare1'
# mapping = {'mappings': {'properties': {'your_field_mappings': {}}}}  # Define your specific field mappings
mapping = {'mappings': {'properties': {'field1': { 'type': 'text' },'field2': { 'type': 'keyword' }}}}
# Perform migration
migrate_mongo_to_es(es_config, mongo_config, collection_name, index_name, mapping)
print("Migration completed successfully!")
