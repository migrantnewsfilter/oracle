from mongomock import MongoClient
from clustering import cluster_updates, cluster_items
from mock import patch, MagicMock
from pymongo import UpdateOne
from datetime import datetime

def test_cluster_items_does_not_throw_when_no_items():
    assert cluster_items([]) == []

def test_cluster_updates_with_empty_db():
    collection = MongoClient().db.collection
    u = cluster_updates(collection, datetime(1970,1,1))
    assert u == []

m = MagicMock(return_value = [2])
@patch('lib.clustering.dbscan', m)
def test_cluster_updates():
    collection = MongoClient().db.collection
    collection.insert_many([
        { '_id': 'tw:abc', 'content': {'body': 'foo'}, 'added': datetime.now()},
        { '_id': 'ge:abc', 'content': {'body': 'foo'}, 'added': datetime.now()},
        { '_id': 'fa:abc', 'content': {'body': 'foo'}, 'added': datetime.now()}
    ])
    u = cluster_updates(collection, datetime(1970, 1,1))
    assert u[0]._doc == {'$set': {'cluster': 10002}}
    assert u[1]._doc == {'$set': {'cluster': 10002}}
    assert u[2]._doc == {'$set': {'cluster': 20002}}
    assert len(u) == 3

def test_cluster_updates_with_malformed_data():
    collection = MongoClient().db.collection
    collection.insert_many([
        { '_id': 'tw:abc', 'content': {'body': 'foo bar baz'}, 'added': datetime.now()},
        { '_id': 'ge:abc', 'added': datetime.now(), 'content': {'body': 'bar baz'}},
        { '_id': 'ge:abc2', 'added': datetime.now()}
    ])

    # We want malformed data to end up in no cluster, cluster 0.
    u = cluster_updates(collection, datetime(1970,1,1))
    assert len(u) == 3
    assert u[1]._doc == {'$set': {'cluster': 0}}
