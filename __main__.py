import logging, os
log_level = getattr(logging, os.environ.get('LOG_LEVEL', 'INFO').upper())
logging.basicConfig(level = logging.DEBUG)

from pymongo import MongoClient
from lib.predictions import write_clusters, write_predictions
from datetime import datetime, timedelta

if __name__ == '__main__':
    client = MongoClient(host = os.environ.get('MONGO_HOST') or None)
    collection = client['newsfilter'].news
    now = datetime.utcnow()

    cluster_start = os.environ.get('ORACLE_CLUSTER_START') or 3
    cluster_end = os.environ.get('ORACLE_CLUSTER_END')
    cluster_window = os.environ.get('ORACLE_CLUSTER_WINDOW') or 7
    cluster_from = now - timedelta(weeks = float(cluster_start))

    if cluster_end:
        cluster_to = now - timedelta(weeks = float(cluster_end))
    else:
        cluster_to = now + timedelta(hours = 1)

    predict_from = now - timedelta(weeks = 3)
    write_predictions(collection, predict_from)
    write_clusters(collection, cluster_from, cluster_to, float(cluster_window))
    client.close()
