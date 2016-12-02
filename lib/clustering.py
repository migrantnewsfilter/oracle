from modelling.clustering import dbscan, get_all_tweets, get_all_articles
from pymongo import MongoClient, UpdateOne

A_PREFIX = 10000
T_PREFIX = 20000

def cluster_items(items):
    bodies = map(lambda x: x['content'].get('body'), items)
    return dbscan(bodies, 3)

def make_cluster_number(num, prefix):
    return 0 if num == -1 else num + prefix

def make_cluster_updates(items, clusters, prefix):
    zipped = zip(items, clusters)
    print zipped[0:10]
    requests = [ UpdateOne({ '_id': item['_id']},
                           {'$set': { 'cluster': make_cluster_number(c, prefix) }})
                 for item, c in zipped]
    return requests

def cluster_updates(client):
    tweets = get_all_tweets(client)
    articles = get_all_articles(client)

    tw_updates = make_cluster_updates(tweets, cluster_items(tweets), T_PREFIX)
    art_updates = make_cluster_updates(articles, cluster_items(articles), A_PREFIX)

    return tw_updates + art_updates
