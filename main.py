import migrate_es_to_mongo
from utils import get_config

if __name__ == "__main__":
    # Replace these values with your Elasticsearch index and MongoDB collection
    es_config, mongo_config = get_config()
    es_index_name = 'shakespeare4'
    mongodb_collection_name = 'shakespeare1'

    migrate_es_to_mongo(es_config, mongo_config, index_name, collection_name)
    print("Migration completed successfully!")
