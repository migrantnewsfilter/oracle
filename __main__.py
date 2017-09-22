import logging, os
log_level = getattr(logging, os.environ.get('LOG_LEVEL', 'INFO').upper())
logging.basicConfig(level = logging.DEBUG)

from pymongo import MongoClient
from lib.predictions import write_clusters, write_predictions
from datetime import datetime, timedelta

if __name__ == '__main__':
    client = MongoClient(host = os.environ.get('MONGO_HOST') or None)
    collection = client['newsfilter'].news
    get_from = datetime.utcnow() - timedelta(weeks = 1) # how long??
    write_predictions(collection, get_from)
    write_clusters(collection, get_from)
    client.close()
