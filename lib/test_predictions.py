from .predictions import write_predictions, write_clusters
from mock import patch, MagicMock
from mongomock import MongoClient
from pymongo import UpdateOne, MongoClient as MC
from datetime import datetime
import pandas as pd
import pytest

@pytest.fixture(scope="module")
def collection():
    client = MC()
    collection = client['newsfilter-test'].news
    yield collection
    collection.drop()
    client.close()


def test_write_predictions_creates_model_and_writes_only_unlabelled(collection):
    model = MagicMock()
    model.predict_proba = MagicMock(return_value = [[.3]])
    create_model = MagicMock(return_value = model)
    with patch('lib.predictions.create_model', create_model) as create_model:
        collection.insert_many([
            {'_id': 'tw:abc', 'title': None, 'content': {'body': 'foo goes to a bar'}, 'label': 'shite', 'added': datetime.utcnow()},
            {'_id': 'tw:cde', 'title': None, 'content': {'body': 'bar walks down the street'}, 'added': datetime.utcnow()}
        ])
        collection.bulk_write = MagicMock()
        write_predictions(collection, datetime(1970,1,1))
        model_data = create_model.call_args[0]
        assert model_data[0].values == ['foo goes to a bar']
        assert model_data[1].values == ['shite']
        model.predict_proba.assert_called_once()
        updates = collection.bulk_write.call_args[0][0]
        assert len(updates) == 1
        assert updates[0]._doc['$set'] == {'prediction': 3}
        collection.drop()


def test_write_predictions_create_model_exception(collection):
    model = MagicMock()
    model.predict_proba = MagicMock(return_value = [[.3]])
    create_model = MagicMock(side_effect = ValueError())
    with patch('lib.predictions.create_model', create_model) as create_model:
        collection.insert_many([
            {'_id': 'tw:cde', 'cluster': 'blah', 'title': 'foo', 'content': {'body': 'bar'}, 'label': 'rejected', 'added': datetime.utcnow()}
        ])
        collection.bulk_write = MagicMock()
        write_predictions(collection, datetime(1970,1,1))
        create_model.assert_called_once()
        model.predict_proba.assert_not_called()
        collection.bulk_write.assert_not_called()
        collection.drop()
