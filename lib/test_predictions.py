from predictions import write_predictions, write_clusters
from mock import patch, MagicMock
from mongomock import MongoClient
from pymongo import UpdateOne
from datetime import datetime

def test_write_predictions():
    model = MagicMock()
    model.predict_proba = MagicMock(return_value = [[.3]])
    create_model = MagicMock(return_value = model)
    with patch('lib.predictions.create_model', create_model) as create_model:
        collection = MongoClient().db.collection
        collection.insert_many([
            {'_id': 'tw:abc', 'content': {'body': 'foo'}, 'label': 'shite', 'added': datetime.utcnow()},
            {'_id': 'tw:cde', 'content': {'body': 'bar'}, 'added': datetime.utcnow()}
        ])
        collection.bulk_write = MagicMock()
        write_predictions(collection, datetime(1970,1,1))
        create_model.assert_called_once_with(['foo'], ['shite'])
        model.predict_proba.assert_called_once()
        updates = collection.bulk_write.call_args[0][0]
        assert len(updates) == 1
        assert updates[0]._doc['$set'] == {'prediction': 3}


def test_write_predictions_create_model_exception():
    # Is this the behavior we want? It writes junk predictions.
    model = MagicMock()
    model.predict_proba = MagicMock(return_value = [[.3]])
    create_model = MagicMock(side_effect = ValueError())
    with patch('lib.predictions.create_model', create_model) as create_model:
        collection = MongoClient().db.collection
        collection.insert_many([
            {'_id': 'tw:cde', 'content': {'body': 'bar'}, 'added': datetime.utcnow()}
        ])
        collection.bulk_write = MagicMock()
        write_predictions(collection, datetime(1970,1,1))
        create_model.assert_called_once_with([], [])
        model.predict_proba.assert_not_called()
        collection.bulk_write.assert_not_called()
