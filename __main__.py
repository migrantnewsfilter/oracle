import os
from pymongo import MongoClient
from lib.predictions import write_clusters, write_predictions

if __name__ == '__main__':
    client = MongoClient(host = os.environ.get('MONGO_HOST') or None)
    collection = client['newsfilter'].news
    write_predictions(collection)
    write_clusters(collection)
    client.close()
