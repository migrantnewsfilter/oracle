from pymongo import UpdateOne
from modelling.clustering import dbscan
from modelling.utils import get_articles

A_PREFIX = 10000
T_PREFIX = 20000

def get_bodies(article):
    try:
        return article['content'].get('body')
    except KeyError as e:
        print "Malformed article in DB!: "
        print e

def cluster_items(items, eps = 0.5):
    if not items:
        return []
    bodies = map(get_bodies, items)
    return dbscan(bodies, eps)

def make_cluster_number(num, prefix):
    return 0 if num == -1 else num + prefix

def make_cluster_updates(items, clusters, prefix):
    zipped = zip(items, clusters)
    requests = [ UpdateOne({ '_id': item['_id']},
                           {'$set': { 'cluster': make_cluster_number(c, prefix) }})
                 for item, c in zipped]
    return requests

def cluster_updates(collection):
    sources = [
        ('ge', A_PREFIX),
        ('fa', A_PREFIX),
        ('tw', T_PREFIX)
    ]
    articles = [(get_articles(collection, src = re), pf) for re,pf in sources]
    updates = [make_cluster_updates(a, cluster_items(a), pf) for a,pf in articles]
    return [x for u in updates for x in u]
