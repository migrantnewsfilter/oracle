from pymongo import UpdateOne
from modelling.clustering import dbscan
from modelling.utils import get_articles
import logging

A_PREFIX = 10000
T_PREFIX = 20000

def get_bodies(article):
    try:
        body =  article['content']['body']
    except KeyError as e:
        logging.error('Malformed article in DB!: ', e)
        return None
    return body

def cluster_items(items, eps = 0.5):
    logging.debug('Clustering {} items...'.format(len(items)))
    if not items:
        return []
    bodies = map(get_bodies, items)
    return dbscan(bodies, eps)

def make_cluster_number(num, prefix):
    return 0 if num == (-1 or 0) else num + prefix

def make_cluster_updates(items, clusters, prefix):
    zipped = zip(items, clusters)
    requests = [ UpdateOne({ '_id': item['_id']},
                           {'$set': { 'cluster': make_cluster_number(c, prefix) }})
                 for item, c in zipped]
    return requests

def cluster_updates(collection, get_from):
    sources = [
        ('ge', A_PREFIX),
        ('fa', A_PREFIX),
        ('tw', T_PREFIX)
    ]
    articles = ((get_articles(collection, src = re, date_start = get_from), pf)
                for re,pf in sources)
    updates = (make_cluster_updates(a, cluster_items(a), pf) for a,pf in articles)
    return (x for u in updates for x in u)
