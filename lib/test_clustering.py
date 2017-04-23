from mongomock import MongoClient
from clustering import cluster_updates, cluster_items
from mock import patch, MagicMock
from pymongo import UpdateOne

def test_cluster_items_does_not_throw_when_no_items():
    assert cluster_items([]) == []

def test_cluster_updates_with_empty_db():
    collection = MongoClient().db.collection
    u = cluster_updates(collection)
    assert u == []

m = MagicMock(return_value = [2])
@patch('lib.clustering.dbscan', m)
def test_cluster_updates():
    collection = MongoClient().db.collection
    collection.insert_many([
        { '_id': 'tw:abc', 'content': {'body': 'foo'}},
        { '_id': 'ge:abc', 'content': {'body': 'foo'}},
        { '_id': 'fa:abc', 'content': {'body': 'foo'}}
    ])
    u = cluster_updates(collection)
    assert u[0]._doc == {'$set': {'cluster': 10002}}
    assert u[1]._doc == {'$set': {'cluster': 10002}}
    assert u[2]._doc == {'$set': {'cluster': 20002}}
    assert len(u) == 3

m = MagicMock(return_value = [2])
@patch('lib.clustering.dbscan', m)
def test_cluster_updates_with_malformed_data():
    # Is this what we want to happen with malformed data???
    # It just tries to cluster it using an empty string??
    collection = MongoClient().db.collection
    collection.insert_many([
        { '_id': 'tw:abc', 'content': {'body': 'foo'}},
        { '_id': 'ge:abc'}
    ])
    u = cluster_updates(collection)
    assert len(u) == 2
