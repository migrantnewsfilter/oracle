from mongomock import MongoClient
from .clustering import cluster_updates, cluster_items
from mock import patch, MagicMock
from pymongo import UpdateOne
from datetime import datetime, timedelta

def test_cluster_items_does_not_throw_when_no_items():
    assert cluster_items([]) == []

def test_cluster_updates_with_empty_db():
    collection = MongoClient().db.collection
    u = cluster_updates(collection, datetime(1970,1,1))
    assert list(u) == []

m = MagicMock(return_value = [2])
@patch('lib.clustering.dbscan', m)
def test_cluster_updates():
    collection = MongoClient().db.collection
    collection.insert_many([
        { '_id': 'tw:abc', 'content': {'body': 'foo'}, 'added': datetime.utcnow()},
        { '_id': 'ge:abc', 'content': {'body': 'foo'}, 'added': datetime.utcnow()}
    ])
    u = list(cluster_updates(collection, datetime(1970, 1,1)))
    assert u[0]._doc == {'$set': {'cluster': 10002}}
    assert u[1]._doc == {'$set': {'cluster': 20002}}
    assert len(u) == 2

def test_cluster_updates_with_malformed_data():
    collection = MongoClient().db.collection
    collection.insert_many([
        { '_id': 'ge:abc', 'content': {'body': 'foo bar baz'}, 'added': datetime.utcnow()},
        { '_id': 'ge:abc3', 'added': datetime.utcnow()}
    ])

    # We want malformed data to end up in no cluster, cluster 0.
    u = list(cluster_updates(collection, datetime(1970,1,1)))
    assert len(u) == 2
    assert u[1]._doc == {'$set': {'cluster': 0}}


m = MagicMock(return_value = [2])
@patch('lib.clustering.dbscan', m)
def test_remove_old_clusters():
    date_from = datetime.now() - timedelta(weeks = 1)
    old_item = datetime.now() - timedelta(weeks = 2)

    collection = MongoClient().db.collection
    collection.insert_many([
        { '_id': 'tw:abc', 'content': {'body': 'foo'}, 'added': datetime.utcnow()},
        { '_id': 'tw:abc1', 'added': old_item, 'cluster': 10002 },
    ])

    u = list(cluster_updates(collection, date_from))
    assert len(u) == 2
    u[0]._doc == { '$set': {'cluster': 10002 }}
    u[1]._doc == { '$set': {'cluster': 0 }}
