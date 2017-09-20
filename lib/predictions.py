from itertools import islice, takewhile, count
from toolz import assoc
from pymongo import UpdateOne
import pandas as pd
import math
from modelling.models import create_model
from modelling.utils import get_articles
from modelling.clustering import get_unique_items
from modelling.fetch import create_df, get_labelled_articles
from clustering import cluster_updates

# TODO: cleanup and make propper logging ini config!
import logging
logger = logging.getLogger()
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
logger.addHandler(ch)


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
    lookup = [('ge', 0.1, 'title'),
              ('tw', 0.5, 'body'),
              ('fa', 0.1, 'body')]
    df = create_df(get_articles(collection, label=True))
    unique = pd.concat([get_unique_items(df[df._id.str.contains(p)], i, k)
                        for p,i,k in lookup])
    return create_model(unique.body, unique.label, [0.5,0.5])

def write_predictions(collection, get_from):
    # If we can't make a model, don't write any predictions
    try:
        model = get_model(collection)
    except Exception:
        logger.exception("Error creating model for predictions.")
        return

    unlabelled = get_articles(collection, label = False, date_start = get_from)
    predicted = (predict_item(item, model) for item in unlabelled)
    chunked = chunk(200, predicted)

    for c in chunked:
        logger.info("Writing prediction update to DB")
        requests = [ UpdateOne({ '_id': obj['_id']},
                               {'$set': { 'prediction': obj['prediction'] }})
                     for obj in c ]
        collection.bulk_write(requests, ordered = False)

def write_clusters(collection, get_from):
    updates = cluster_updates(collection, get_from)
    chunked = chunk(200, updates)
    for c in chunked:
        logger.info("Writing cluster update to DB")
        collection.bulk_write(c, ordered = False)
