from itertools import islice, takewhile, count
from toolz import assoc
from pymongo import UpdateOne
import math
from modelling.models import create_model
from modelling.utils import get_articles
from clustering import cluster_updates

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
    l = get_articles(collection, label = True)
    labels = map(lambda x: x['label'], l)
    bodies = map(lambda x: x['content']['body'], l)
    return create_model(bodies, labels)

def write_predictions(collection):
    unlabelled = get_articles(collection, label = False)
    # If we can't make a model, don't write any predictions
    try:
        model = get_model(collection)
    except ValueError as e:
        print e
        return

    predicted = (predict_item(item, model) for item in unlabelled)
    chunked = chunk(200, predicted)

    for c in chunked:
        requests = [ UpdateOne({ '_id': obj['_id']},
                               {'$set': { 'prediction': obj['prediction'] }})
                     for obj in c ]
        collection.bulk_write(requests, ordered = False)

def write_clusters(collection):
    updates = cluster_updates(collection)
    chunked = chunk(200, updates)
    for c in chunked:
        collection.bulk_write(c, ordered = False)