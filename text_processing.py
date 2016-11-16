from stop_words import get_stop_words
from nltk.stem.porter import PorterStemmer
from textblob import TextBlob

def split_into_tokens(text):
    #split a message into its individual words
    #text = unicode(text, 'utf8') - convert bytes into proper unicode - does not work because already unicode
    return TextBlob(text).words

def remove_stop_words(text):
    #remove stop words
    en_stop = get_stop_words('en')
    #sp_stop = get_stop_words('Spanish')
    return [word for word in text if word not in en_stop]

def stemming_words(text):
    #Stemm words
    p_stemmer = PorterStemmer()
    return [p_stemmer.stem(i) for i in text]

def split_into_lemmas(text):
    #normalize words into their base form (lemmas)
    text = text.lower()
    words = TextBlob(text).words
    # for each word, take its "base form" = lemma
    return [word.lemma for word in words]
