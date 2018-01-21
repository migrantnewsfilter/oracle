from itertools import islice, takewhile, count
from toolz import assoc
from pymongo import UpdateOne
import pandas as pd
import math
from modelling.models import get_prediction_data, train_and_predict
from modelling.utils import get_articles
# from modelling.clustering import get_unique_items
from modelling.fetch import create_df, get_labelled_articles
from .clustering import cluster_updates
import logging

def chunk(n, it):
    src = iter(it)
    return takewhile(bool, (list(islice(src, n)) for _ in count(0)))

def normalize_prediction(prediction):
    return int(math.floor(prediction*10))

def write_predictions(collection, get_from):
    # If we can't make a model, don't write any predictions
    X_train, y_train, _ = get_prediction_data(collection, label=True)
    X_new, _, ids = get_prediction_data(collection, label=False, start=get_from)

    logging.debug('Predicting {} items from training size {}'.format(len(X_new), len(X_train)))
    try:
        preds = train_and_predict(X_train, y_train, X_new)
    except Exception as e:
        logging.error('Error predicting data: ', e)
        return

    chunked = chunk(200, zip(ids, map(normalize_prediction, preds)))
    for c in chunked:
        logging.debug("Writing prediction update to DB")
        requests = [ UpdateOne({ '_id': i},
                               {'$set': { 'prediction': p }})
                     for i,p in c ]
        collection.bulk_write(requests, ordered = False)

def write_clusters(collection, start, end, cluster_window):
    updates = cluster_updates(collection, start, end, cluster_window)
    chunked = chunk(200, updates)
    for c in chunked:
        logging.debug("Writing cluster update to DB")
        collection.bulk_write(c, ordered = False)
