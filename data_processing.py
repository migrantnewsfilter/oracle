#%matplotlib inline
import os
import csv
import json
from bson.json_util import dumps
from pymongo import MongoClient, ASCENDING

##GENERAL PACKAGES
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import cPickle

##TEXT MINING PACKAGES
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
#from scipy import sparse, io
#import HTMLParser
from text_processing import *

path = './Data/MMP_all_data.csv'

################################################################################
############################LOADING IN OF DATA SET##############################
################################################################################
#This section pulls the current data from the MongoDB database
#Afterwards the raw json data is filed into a pandas dataframe
#In the end a text variable is created which concatenates the relevant data used in the following text mining exercise

def get_articles():
    client = MongoClient("mongodb://209.177.92.45:80")
    collection = client['newsfilter'].news #news - is new one
    cursor = collection.find().sort('added', ASCENDING)
    news_feeds_json = dumps(cursor)
    news_feeds = pd.read_json(news_feeds_json)
    news_feeds_df = pd.DataFrame(news_feeds)
    return news_feeds_df

#data = get_articles()
################################################################################
#############################FEATURE GENERATION#################################
################################################################################

def feature_gen():
    news_feeds_df = get_articles()
    temp = news_feeds_df['content'].apply(pd.Series)
    news_feeds_df = pd.concat([news_feeds_df, temp], axis=1)
    news_feeds_df['text'] =  news_feeds_df['title'] + " " + news_feeds_df['body']
    #html_parser = HTMLParser.HTMLParser()
    #news_feeds_df['text'] = html_parser.unescape(news_feeds_df['text'])
    news_feeds_df = news_feeds_df.dropna(subset=['label', 'text'])
    news_feeds_df['text'] = news_feeds_df['text'].str.replace("<b>", "", n=-1, case=True, flags=0)
    news_feeds_df['text'] = news_feeds_df['text'].str.replace("</b>", "", n=-1, case=True, flags=0)
    news_feeds_df['text'] = news_feeds_df['text'].str.replace("&#39", "", n=-1, case=True, flags=0)
    news_feeds_df['text'] = news_feeds_df['text'].str.replace("/x", "", n=-1, case=True, flags=0)
    return news_feeds_df

#data_feat = feature_gen()

################################################################################
############################MERGE SUPPL DATA MMP################################
################################################################################


def merge_mmp(path):
    news_feeds_df = feature_gen()
    MMP_supplementary = pd.DataFrame.from_csv(path, sep=',', encoding='utf-8')
    #len(MMP_supplementary)
    #list(MMP_supplementary.columns.values)
    MMP_supplementary['label'] = 'accepted'
    #MMP_supplementary['INFORELIABILITY'] == 'Verified'
    MMP_supplementary['COMMENT'] = MMP_supplementary['COMMENT'].fillna('')
    MMP_supplementary['Cause_of_death'] = MMP_supplementary['Cause_of_death'].fillna('')
    MMP_supplementary['text'] = MMP_supplementary['COMMENT'] + " " + MMP_supplementary['Cause_of_death']
    MMP_supplementary['text'].dropna
    MMP_supplementary = MMP_supplementary[MMP_supplementary['text'] == 'Unknown']
    MMP_supplementary = MMP_supplementary[['label', 'text']]
    news_feeds_df = news_feeds_df[['label', 'text']]
    news_feeds_df = news_feeds_df.append(MMP_supplementary, ignore_index=True)
    news_feeds_df = news_feeds_df.dropna(subset=['label', 'text'])
    return news_feeds_df

#path = '/Users/robertlange/Desktop/news_filter_project/Modelling/Data/MMP_all_data.csv'
#news_feeds_df = merge_mmp(path)

################################################################################
##############################OVERVIEW OF DATA SET##############################
################################################################################

def overview(data):
    print  "Size of data set after dropping unlabelled:", len(news_feeds_df)
    news_feeds_df['length'] = news_feeds_df['text'].map(lambda text: len(text))
    news_feeds_df.length.plot(bins=20, kind='hist')
    plt.savefig('Graphical_Analysis/Histogram_Text_Length.png')
    news_feeds_df.hist(column='length', by='label', bins=50)
    plt.savefig('Graphical_Analysis/Histogram_Text_Length_Label.png')

#overview(merge_mmp(path))

################################################################################
#################################TEXT PROCESSING################################
################################################################################

def clean_text(data):
    news_feeds_df = data
    NF_df_tokens = news_feeds_df.text.apply(split_into_tokens)
    NF_df_lemmas = news_feeds_df.text.apply(split_into_lemmas)
    NF_df_lemmas_stop = NF_df_lemmas.apply(remove_stop_words)
    #NF_df_lemmas_stop_stem = NF_df_lemmas_stop.apply(stemming_words)
    print NF_df_tokens.head(5)
    print NF_df_lemmas.head(5)
    print NF_df_lemmas_stop.head(5)
    #print NF_df_lemmas_stop_stem.text.head(5)
    return NF_df_lemmas_stop

#clean_text(merge_mmp(path))
######################################################
###############VECTOR TRANSFORMATIONS ################
######################################################
#This section applies different transformations to the processed text data
#The goal is to obtain vector representations of frequencies of words in text
#We introduce 2 different transdformations: Simple Bag of Words (bow) and Ngram
#Each vector has as many dimensions as there are unique words (or word combinations) in the text corpus

def vector_trafo(data, trafo):
    news_feeds_df = data
    if trafo == 1:
        bow_transformer = CountVectorizer(analyzer=split_into_lemmas).fit(news_feeds_df['text'])
        BOW = bow_transformer.transform(news_feeds_df['text'])
        print 'sparse matrix shape - BOW representation:', BOW.shape #dim: number feeds x unique words
        return BOW
    else:
        ngram_transformer = CountVectorizer(ngram_range=(1, trafo), token_pattern=r'\b\w+\b', min_df=1)
        NGRAM = ngram_transformer.fit_transform(news_feeds_df['text']).toarray()
        print 'sparse matrix shape - Ngram representation:', NGRAM.shape #dim: number feeds x unique words and 2-word combinations
        return NGRAM


#vector_trafo(merge_mmp('/Users/robertlange/Desktop/news_filter_project/Modelling/Data/MMP_all_data.csv'),1)

#More detailed information
#print len(bigram_transformer.vocabulary_)
#print len(bow_transformer.vocabulary_)
#print 'number of non-zeros:', news_feeds_df_bow.nnz
#print 'sparsity: %.2f%%' % (100.0 * news_feeds_df_bow.nnz / (news_feeds_df_bow.shape[0] * news_feeds_df_bow.shape[1]))

######################################################
################tfidf - transformation################
######################################################
#This section implements term weighting and normalization by applying (TF-IDF)
#Choose which dimensionality of the text to use in further analysis (here bigram)

def tfidf_trafo(data,trafo):
        news_feeds_df_textmodel = vector_trafo(data, trafo)
        tfidf_transformer = TfidfTransformer().fit(news_feeds_df_textmodel)
        news_feeds_df_tfidf = tfidf_transformer.transform(news_feeds_df_textmodel)
        news_feeds_df_tfidf = news_feeds_df_tfidf.tocsr()
        news_feeds_df_tfidf = news_feeds_df_tfidf.toarray()
        print 'sparse matrix shape - TF-IDF representation (ngram):', news_feeds_df_tfidf.shape
        return news_feeds_df_tfidf

#tfidf_trafo(merge_mmp('/Users/robertlange/Desktop/news_filter_project/Modelling/Data/MMP_all_data.csv'), 1)

######################################################
#######################Outfiling######################
######################################################
#Outfile the current working data
#io.mmwrite('Data/data_tfidf_' + time.strftime("%Y_%m_%d") +".mtx", news_feeds_df_tfidf)
#news_feeds_df.to_csv('Data/data_df_' + time.strftime("%Y_%m_%d") +".csv", sep='\t', encoding='utf-8')
