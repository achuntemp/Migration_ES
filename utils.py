import json

def get_config():
    # Read configuration file
    with open("config.json", "r") as f:
        config = json.load(f)

    # Extract Elasticsearch and MongoDB configurations
    es_config = config["elastic_search"]
    mongo_config = config["mongo_db"]

    return es_config, mongo_config