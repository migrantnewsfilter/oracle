import logging, os
log_level = getattr(logging, os.environ.get('LOG_LEVEL', 'INFO').upper())
logging.basicConfig(level = logging.DEBUG)

from pymongo import MongoClient
from lib.predictions import write_clusters, write_predictions
from datetime import datetime, timedelta

if __name__ == '__main__':
    client = MongoClient(host = os.environ.get('MONGO_HOST') or None)
    collection = client['newsfilter'].news
    cluster_weeks = os.environ.get('ORACLE_CLUSTER_WEEKS') or 3
    cluster_from = datetime.utcnow()- timedelta(weeks = cluster_weeks)
    predict_from = datetime.utcnow() - timedelta(weeks = 3)
    write_predictions(collection, predict_from)
    write_clusters(collection, cluster_from)
    client.close()
