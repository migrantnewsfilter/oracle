from predictions import write_predictions, write_clusters
from mock import patch, MagicMock
from mongomock import MongoClient
from pymongo import UpdateOne

def test_write_predictions():
    model = MagicMock()
    model.predict_proba = MagicMock(return_value = [[.3]])
    create_model = MagicMock(return_value = model)
    with patch('lib.predictions.create_model', create_model) as create_model:
        collection = MongoClient().db.collection
        collection.insert_many([
            {'_id': 'tw:abc', 'content': {'body': 'foo'}, 'label': 'shite'},
            {'_id': 'tw:cde', 'content': {'body': 'bar'}}
        ])
        collection.bulk_write = MagicMock()
        write_predictions(collection)
        create_model.assert_called_once_with(['foo'], ['shite'])
        model.predict_proba.assert_called_once()
        updates = collection.bulk_write.call_args[0][0]
        assert len(updates) == 1
        assert updates[0]._doc['$set'] == {'prediction': 3}


def test_write_predictions_nothing_labelled():
    # Is this the behavior we want? It writes junk predictions.
    model = MagicMock()
    model.predict_proba = MagicMock(return_value = [[.3]])
    create_model = MagicMock(return_value = model)
    with patch('lib.predictions.create_model', create_model) as create_model:
        collection = MongoClient().db.collection
        collection.insert_many([
            {'_id': 'tw:cde', 'content': {'body': 'bar'}}
        ])
        collection.bulk_write = MagicMock()
        write_predictions(collection)
        create_model.assert_called_once_with([], [])
        model.predict_proba.assert_called_once()
        updates = collection.bulk_write.call_args[0][0]
        assert len(updates) == 1
        assert updates[0]._doc['$set'] == {'prediction': 3}
