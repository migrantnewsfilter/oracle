from itertools import islice, takewhile, count
from toolz import assoc
import schedule
from pymongo import MongoClient, UpdateOne
from textblob import TextBlob
import cPickle, os, time, math
import boto3

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

def get_model(aws_client):

    # Get the latest model from our bucket
    try:
        objects = aws_client.list_objects(Bucket = 'migrantnews-app-models')['Contents']
        newest = reduce(lambda a,b: a if a['LastModified'] > b['LastModified'] else b, objects)
    except KeyError as e:
        raise Exception('Could not find a model!', e)

    # read and unpickle the model!
    obj = aws_client.get_object(Bucket = 'migrantnews-app-models', Key=newest.get('Key'))
    return cPickle.loads(obj['Body'].read())


def write_predictions():
    client = MongoClient(
        host = os.environ.get('MONGO_HOST') or None
    )
    collection = client['newsfilter'].news
    unlabelled = get_unlabelled(client)

    aws_client = boto3.client('s3')
    model = get_model(aws_client)

    predicted = (predict_item(item, model) for item in unlabelled)
    chunked = chunk(20, predicted)

    for c in chunked:
        print 'ORACLE: writing predictions to DB'
        requests = [ UpdateOne({ '_id': obj['_id']},
                               {'$set': { 'prediction': obj['prediction'] }})
                     for obj in c ]
        collection.bulk_write(requests, ordered=False)

    client.close()


if __name__ == '__main__':
    # write once on startup
    write_predictions()

    # schedule to run again
    schedule.every(5).minutes.do(write_predictions)

    # run the scheduler!
    while True:
        schedule.run_pending()
        time.sleep(10)
