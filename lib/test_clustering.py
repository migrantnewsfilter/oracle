from mongomock import MongoClient
from .clustering import *
from mock import patch, MagicMock
from pymongo import UpdateOne
from datetime import datetime, timedelta

def test_cluster_updates_with_empty_db():
    collection = MongoClient().db.collection
    u = cluster_updates(collection, datetime(1970,1,1))
    assert list(u) == []

m = MagicMock(return_value = [2])
@patch('lib.clustering.dbscan', m)
def test_cluster_updates():
    collection = MongoClient().db.collection
    collection.insert_many([
        { '_id': 'tw:abc', 'content': {'body': 'foo'}, 'published': datetime.utcnow(), 'added': datetime.utcnow()},
        { '_id': 'ge:abc', 'content': {'body': 'foo', 'title': 'foo'}, 'published': datetime.utcnow(), 'added': datetime.utcnow()}
    ])
    u = list(cluster_updates(collection, datetime.utcnow() - timedelta(days = 14)))
    assert u[0]._doc == {'$set': {'cluster': 'acbd18db4cc2f85cedef654fccc4a4d8'}}
    assert u[1]._doc == {'$set': {'cluster': 'acbd18db4cc2f85cedef654fccc4a4d8'}}
    assert len(u) == 2

def test_cluster_updates_with_malformed_data():
    collection = MongoClient().db.collection
    collection.insert_many([
        { '_id': 'ge:abc', 'content': {'title': 'foo', 'body': 'foo bar baz'}, 'added': datetime.utcnow()},
        { '_id': 'ge:abc3', 'added': datetime.utcnow()}
    ])

    # malformed data doesn't get a cluster. Both updates are from well-formed item
    u = list(cluster_updates(collection, datetime.utcnow() - timedelta(weeks = 12)))
    assert len(u) == 2
    assert(u[0]._filter['_id'] == 'ge:abc')
    assert(u[1]._filter['_id'] == 'ge:abc')


def test_get_windowed_articles_with_windows():
    date_from = datetime.now() - timedelta(weeks = 7)
    old = datetime.now() - timedelta(weeks = 2)
    older = datetime.now() - timedelta(weeks = 4)
    oldest = datetime.now() - timedelta(weeks = 6)

    collection = MongoClient().db.collection
    collection.insert_many([{ '_id': 'tw:a', 'added': datetime.utcnow()},
                            {'_id': 'tw:b', 'added': old},
                            {'_id': 'tw:c', 'added': older},
                            {'_id': 'tw:d', 'added': oldest}])

    wins = [list(w) for w in get_windowed_articles(collection, 14, start = date_from)]
    assert len(wins) == 6


#########################################################
# get_windows
#########################################################

def test_get_windows_even():
    start = datetime(2017,10,1)
    windows = get_windows(start, start + timedelta(days = 30), 10, 5)
    assert len(windows) == 5
    assert windows[0][0] == start
    assert windows[0][1] == start + timedelta(10)
    assert windows[1][0] == start + timedelta(5)
    assert windows[1][1] == start + timedelta(15)
    assert windows[4][1] == start + timedelta(30)


def test_get_windows_odd():
    start = datetime(2017,10,1)
    windows = get_windows(start, start + timedelta(days = 29, minutes = 10), 10, 5)
    assert len(windows) == 5
    assert windows[4][0] == start + timedelta(20)
    assert windows[4][1] == start + timedelta(days = 29, minutes = 10)
    windows = get_windows(start, start + timedelta(days = 30, hours = 5, minutes = 2), 10, 5)
    assert len(windows) == 6
    assert windows[5][1] == start + timedelta(days = 30, hours = 5, minutes = 2)
