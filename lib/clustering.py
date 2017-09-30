from pymongo import UpdateOne
from modelling.clustering import dbscan
from modelling.utils import get_articles
from sklearn.cluster import DBSCAN
from itertools import chain
import logging
import os

A_PREFIX = 10000
T_PREFIX = 20000

def get_bodies(article):
    try:
        body =  article['content']['body']
    except KeyError as e:
        # logging.error('Malformed article in DB!: ', e)
        return None
    return body

def cluster_items(items, eps = 0.5):
    logging.debug('Clustering {} items...'.format(len(items)))
    if not items:
        return []
    bodies = map(get_bodies, items)
    db = DBSCAN(eps, min_samples = 2, algorithm = 'brute', metric = 'cosine')
    return dbscan(bodies, db)

def make_cluster_number(num, prefix):

    # num will by numpy.int64, which we cast to int
    return 0 if num == (-1 or 0) else int(num + prefix)

def make_cluster_updates(items, clusters, prefix):
    zipped = zip(items, clusters)
    requests = [ UpdateOne({ '_id': item['_id']},
                           {'$set': { 'cluster': make_cluster_number(c, prefix) }})
                 for item, c in zipped]
    return requests

def make_cluster_removal(item):
    return UpdateOne({ '_id': item['_id']},
                     {'$set': { 'cluster': 0 }})

def cluster_updates(collection, get_from):
    sources = [
        ('ge', A_PREFIX, os.environ.get('ORACLE_GE_EPS') or 0.5),
        ('fa', A_PREFIX, 0.5),
        ('tw', T_PREFIX, os.environ.get('ORACLE_TW_EPS') or 0.5)
    ]
    articles = ((get_articles(collection, src = re, date_start = get_from),pf,e)
                for re,pf,e in sources)
    old_articles = get_articles(collection, date_end = get_from)
    removals = (make_cluster_removal(a) for a in old_articles)
    updates = (make_cluster_updates(a, cluster_items(a,e), pf) for a,pf,e in articles)
    updates = (x for u in updates for x in u)
    return chain(updates, removals)
