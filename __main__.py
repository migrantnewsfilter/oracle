from itertools import islice, takewhile, count
from toolz import assoc
from pymongo import MongoClient, UpdateOne
import cPickle, os, time, math
import boto3
from modelling.models import get_labelled_articles, create_model
from lib.clustering import cluster_updates

def chunk(n, it):
    src = iter(it)
    return takewhile(bool, (list(islice(src, n)) for _ in count(0)))

def get_unlabelled(client):
    collection = client['newsfilter'].news
    return collection.find({ 'label': None })

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

def get_model(client):
    l = get_labelled_articles(client)
    labels = map(lambda x: x['label'], l)
    bodies = map(lambda x: x['content']['body'], l)
    return create_model(bodies, labels)

def write_predictions():
    client = MongoClient(
        host = os.environ.get('MONGO_HOST') or None
    )
    collection = client['newsfilter'].news
    unlabelled = get_unlabelled(client)

    model = get_model(client)

    predicted = (predict_item(item, model) for item in unlabelled)
    chunked = chunk(200, predicted)

    for c in chunked:
        print 'ORACLE: writing predictions to DB'
        requests = [ UpdateOne({ '_id': obj['_id']},
                               {'$set': { 'prediction': obj['prediction'] }})
                     for obj in c ]
        collection.bulk_write(requests, ordered=False)

    client.close()

def write_clusters():
    client = MongoClient(
        host = os.environ.get('MONGO_HOST') or None
    )
    collection = client['newsfilter'].news

    updates = cluster_updates(client)
    chunked = chunk(200, updates)
    for c in chunked:
        print 'ORACLE: writing clusters to DB'
        collection.bulk_write(c, ordered = False)
    client.close()

if __name__ == '__main__':
    write_predictions()
    write_clusters()
