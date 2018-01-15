from pymongo import UpdateOne
from modelling.clustering import dbscan, cluster_articles
from modelling.utils import get_articles, get_bodies, md5
from sklearn.cluster import DBSCAN
from itertools import chain
from datetime import datetime, timedelta
import logging, os
from math import ceil

def get_windows(start, end, window, overlap):

    # Basic calculations
    inc = window - overlap
    delta = end - start
    win = timedelta(window)

    # Add a day if there are any seconds in the timedelta (round up!)
    period = delta.days + 1 if delta.seconds > 0 else delta.days
    span = int(ceil((period / inc)))

    # calculate periods and create tuples
    new_start = lambda i: start + timedelta(inc*i)
    new_end = lambda s: s + win if s + win < end else end
    return [(new_start(i), new_end(new_start(i)))
            for i in range(span - 1)]

def get_windowed_articles(coll, window,
                          label = None,
                          src = '',
                          start = datetime(1970, 1, 1),
                          end = datetime.utcnow() + timedelta(hours = 1)):

    wins = get_windows(start, end, window, ceil(window/2))
    return (get_articles(coll, label, src, s, e) for s,e in wins)

def make_cluster_updates(items, clusters):
    zipped = zip(items, clusters)
    requests = [ UpdateOne({ '_id': item['_id']},
                           {'$set': { 'cluster': c }})
                 for item, c in zipped]
    return requests

def make_cluster_removal(item):
    return UpdateOne({ '_id': item['_id']},
                     {'$set': { 'cluster': 0 }})

def cluster_updates(collection, get_from, window = 7):
    sources = [
        ('ge', os.environ.get('ORACLE_GE_EPS') or 0.5, 'title'),
        ('fa', 0.5, 'body'),
        ('tw', os.environ.get('ORACLE_TW_EPS') or 0.5, 'body')
    ]

    # get windows per source
    articles = ((list(a),e,b) for re,e,b in sources for a in
                get_windowed_articles(collection, window, src=re, start=get_from))
    updates = (make_cluster_updates(a, cluster_articles(a,e,b)) for a,e,b in articles)
    updates = (x for u in updates for x in u)
    return updates
