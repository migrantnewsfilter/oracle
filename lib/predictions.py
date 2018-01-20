from itertools import islice, takewhile, count
from toolz import assoc
from pymongo import UpdateOne
import pandas as pd
import math
from modelling.models import create_model
from modelling.utils import get_articles
# from modelling.clustering import get_unique_items
from modelling.fetch import create_df, get_labelled_articles
from .clustering import cluster_updates
import logging

def chunk(n, it):
    src = iter(it)
    return takewhile(bool, (list(islice(src, n)) for _ in count(0)))

def make_prediction(item, model):
    try:
        body = item['content']['body']
        return model.predict_proba([body])[0][0]
    except KeyError:
        return None

def normalize_prediction(prediction):
    return int(math.floor(prediction*10))

def predict_item(item, model):
    prediction = normalize_prediction(make_prediction(item, model))
    return assoc(item, 'prediction', prediction)

def get_model(collection):
    logging.debug('Getting data for model for prediction')

    # TODO: ge --> with title or body??
    lookup = [('ge', 'title'),
              ('tw', 'body'),
              ('fa', 'body')]

    articles = get_articles(collection, label = True, unique = True)
    articles = list(articles)
    if not articles:
        raise Exception('Could not find any labelled articles in Database')

    df = create_df(articles)
    logging.debug('Creating model for prediction')
    return create_model(df.body, df.label, [0.5,0.5])

def write_predictions(collection, get_from):
    # If we can't make a model, don't write any predictions
    try:
        model = get_model(collection)
    except Exception as e:
        logging.exception('Error creating model for predictions: {}'.format(e))
        return

    logging.debug('Predicting items.')
    unlabelled = get_articles(collection, label = False, date_start = get_from, unique = True)
    predicted = (predict_item(item, model) for item in unlabelled)
    chunked = chunk(200, predicted)

    for c in chunked:
        logging.debug("Writing prediction update to DB")
        requests = [ UpdateOne({ '_id': obj['_id']},
                               {'$set': { 'prediction': obj['prediction'] }})
                     for obj in c ]
        collection.bulk_write(requests, ordered = False)

def write_clusters(collection, get_from, cluster_window):
    updates = cluster_updates(collection, get_from, cluster_window)
    chunked = chunk(200, updates)
    for c in chunked:
        logging.debug("Writing cluster update to DB")
        collection.bulk_write(c, ordered = False)
