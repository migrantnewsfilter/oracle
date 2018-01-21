from .predictions import write_predictions, write_clusters
from mock import patch, MagicMock
from mongomock import MongoClient
from pymongo import UpdateOne, MongoClient as MC
from datetime import datetime
import pandas as pd
import numpy as np
import pytest

@pytest.fixture(scope="module")
def collection():
    client = MC()
    collection = client['newsfilter-test'].news
    yield collection
    collection.drop()
    client.close()


def test_write_predictions_creates_model_and_writes_only_unlabelled(collection):
    m = MagicMock(return_value = np.array([.34, .5]))
    with patch('lib.predictions.train_and_predict', m) as tp:
        collection.insert_many([
            {'_id': 'tw:abc', 'title': None, 'content': {'body': 'foo goes to a bar'}, 'label': 'accepted', 'added': datetime.utcnow()},
            {'_id': 'tw:cde', 'title': None, 'content': {'body': 'bar walks down the street'}, 'added': datetime.utcnow()},
            {'_id': 'tw:efg', 'title': None, 'content': {'body': 'bar too town'}, 'added': datetime.utcnow()}
        ])
        collection.bulk_write = MagicMock()
        write_predictions(collection, datetime(1970,1,1))
        model_data = tp.call_args[0]
        assert model_data[0].values == ['foo goes to a bar']
        assert model_data[1].values == [True]
        updates = collection.bulk_write.call_args[0][0]
        assert len(updates) == 2
        assert updates[0]._doc['$set'] == {'prediction': 3}
        assert updates[1]._doc['$set'] == {'prediction': 5}
        collection.drop()


def test_write_predictions_create_model_exception(collection):
    model = MagicMock()
    model.predict_proba = MagicMock(return_value = [[.3]])
    train_and_predict = MagicMock(side_effect = ValueError())
    with patch('lib.predictions.train_and_predict', train_and_predict) as tp:
        collection.insert_many([
            {'_id': 'tw:cde', 'cluster': 'blah', 'title': 'foo', 'content': {'body': 'bar'}, 'label': 'rejected', 'added': datetime.utcnow()}
        ])
        collection.bulk_write = MagicMock()
        write_predictions(collection, datetime(1970,1,1))
        tp.assert_called_once()
        model.predict_proba.assert_not_called()
        collection.bulk_write.assert_not_called()
        collection.drop()
