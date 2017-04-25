from pymongo import MongoClient
import os

def label_em(collection, n, label):
    rands = list(collection.find().limit(n))
    ids = [r['_id'] for r in rands]
    return [collection.update_one({'_id': i}, {'$set': {'label': label}}) for i in ids]

def label_randomly(n):
    client = MongoClient(host = os.environ.get('MONGO_HOST'))
    collection = client['newsfilter'].news
    label_em(collection, n, 'rejected')
    label_em(collection, int(round(n/2)), 'accepted')
    client.close()


def test_database_fresh():
    client = MongoClient(host = os.environ.get('MONGO_HOST'))
    collection = client['newsfilter'].news
    assert collection.find().count() == collection.find({ 'prediction': None}).count()
    client.close()

def test_predictions_present():
    client = MongoClient(host = os.environ.get('MONGO_HOST'))
    collection = client['newsfilter'].news
    assert collection.find({ 'label': None }).count() == collection.find({ 'prediction': {'$exists': True}}).count()
    client.close()

def test_clusters_present():
    client = MongoClient(host = os.environ.get('MONGO_HOST'))
    collection = client['newsfilter'].news
    assert collection.find().count() == collection.find({ 'cluster': {'$exists': True}}).count()
    client.close()
