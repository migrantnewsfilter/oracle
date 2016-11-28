from pymongo import MongoClient
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.cluster import DBSCAN

def get_articles_by_source(src = '', label = ''):
    client = MongoClient()
    collection = client['newsfilter'].news #news - is new one
    cursor = collection.find({ '_id': {'$regex': src }, 'label': {'$regex': label}})
    return list(cursor)

def get_all_articles():
    real = get_articles_by_source('ge')
    fake = get_articles_by_source('fa')
    return real + fake

def get_all_tweets():
    tweets = get_articles_by_source('tw')
    return tweets

def dbscan(data, epsilon):
    vector = CountVectorizer(
        stop_words='english',
        ngram_range=(2,3)
    ).fit_transform(data)

    db = DBSCAN(eps=epsilon, min_samples=2)
    fit = db.fit_predict(vector)
    return fit
