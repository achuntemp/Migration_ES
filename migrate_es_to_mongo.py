
from elasticsearch import Elasticsearch
from pymongo import MongoClient
from utils import get_config

def migrate_es_to_mongo(es_config, mongo_config, index_name, collection_name):
    # Initialize clients

    # Initialize Elasticsearch client
    es_client = Elasticsearch(hosts=es_config['hosts'], basic_auth=("elastic", "ybY9LVP=Jqmbx5lYgMGF"))
    # es_client = Elasticsearch(hosts=es_config['hosts'])
    mongo_client = MongoClient(mongo_config['uri'])
    db = mongo_client[mongo_config['database']]
    collection = db[collection_name]

    # Iterate through documents and insert into MongoDB collection
    # Perform the Elasticsearch search
    es_query = {
        "query": {
            "match_all": {}
        }
    }
    es_results = es_client.search(index=index_name, body=es_query, scroll='2m', size=1000)

    # Iterate through the Elasticsearch results and insert into MongoDB
    while len(es_results['hits']['hits']) > 0:
        for hit in es_results['hits']['hits']:
            mongodb_document = hit['_source']
            collection.insert_one(mongodb_document)

        # Scroll to the next batch of results
        es_results = es_client.scroll(scroll_id=es_results['_scroll_id'], scroll='2m')



# Get configuration details
es_config, mongo_config = get_config()

# Specify index name and collection name
index_name = 'shakespeare4'
collection_name = 'shakespeare1'

if __name__ == "__main__":
    # Replace these values with your Elasticsearch index and MongoDB collection
    es_index_name = 'shakespeare4'
    mongodb_collection_name = 'shakespeare1'

    migrate_es_to_mongo(es_config, mongo_config, index_name, collection_name)
    print("Migration completed successfully!")
