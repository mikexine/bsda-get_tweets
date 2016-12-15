#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
forked from mentzera --> from AWS/Twitter/ES tutorial
'''
import re
import time
from textblob import TextBlob

class Sentiments:
    POSITIVE = 'Positive'
    NEGATIVE = 'Negative'
    NEUTRAL = 'Neutral'
    CONFUSED = 'Confused'
    
id_field = 'id'
emoticons = {Sentiments.POSITIVE:'😀|😁|😂|😃|😄|😅|😆|😇|😈|😉|😊|😋|😌|😍|😎|😏|😗|😘|😙|😚|😛|😜|😝|😸|😹|😺|😻|😼|😽',
             Sentiments.NEGATIVE : '😒|😓|😔|😖|😞|😟|😠|😡|😢|😣|😤|😥|😦|😧|😨|😩|😪|😫|😬|😭|😾|😿|😰|😱|🙀',
             Sentiments.NEUTRAL : '😐|😑|😳|😮|😯|😶|😴|😵|😲',
             Sentiments.CONFUSED: '😕'
             }

tweet_mapping = {'properties': 
                    {'timestamp_ms': {
                                  'type': 'date'
                                  },
                     'text': {
                                  'type': 'string'
                              },
                     'language': {
                                  'type': 'string'
                              },
                     'source': {
                                  'type': 'string'
                              },
                     'coordinates': {
                          'properties': {
                             'coordinates': {
                                'type': 'geo_point'
                             },
                             'type': {
                                'type': 'string',
                                'index' : 'not_analyzed'
                            }
                          }
                     },
                     'user': {
                          'properties': {
                             'id': {
                                'type': 'long'
                             },
                             'followers': {
                                'type': 'long'
                             },
                             'friends': {
                                'type': 'long'
                             },
                             'favourites': {
                                'type': 'long'
                             },
                             'statuses': {
                                'type': 'long'
                             },
                             'name': {
                                'type': 'string'
                             },
                             'created_at': {
                                'type': 'date'
                             },
                             'time_zone': {
                                'type': 'string'
                             },
                             'verified': {
                                'type': 'boolean'
                             },
                          }
                     },
                     'sentiments': {
                                  'type': 'string',
                                  'index' : 'not_analyzed'
                              }
                    }
                 }




def _sentiment_analysis(tweet):
    tweet['emoticons'] = []
    tweet['sentiments'] = []
    _sentiment_analysis_by_emoticons(tweet)
    if len(tweet['sentiments']) == 0:
        _sentiment_analysis_by_text(tweet)


def _sentiment_analysis_by_emoticons(tweet):
    for sentiment, emoticons_icons in emoticons.items():
        matched_emoticons = re.findall(emoticons_icons, tweet['text'])
        if len(matched_emoticons) > 0:
            tweet['emoticons'].extend(matched_emoticons)
            tweet['sentiments'].append(sentiment)
    
    if Sentiments.POSITIVE in tweet['sentiments'] and Sentiments.NEGATIVE in tweet['sentiments']:
        tweet['sentiments'] = Sentiments.CONFUSED
    elif Sentiments.POSITIVE in tweet['sentiments']:
        tweet['sentiments'] = Sentiments.POSITIVE
    elif Sentiments.NEGATIVE in tweet['sentiments']:
        tweet['sentiments'] = Sentiments.NEGATIVE

def _sentiment_analysis_by_text(tweet):
    blob = TextBlob(tweet['text'])
    sentiment_polarity = blob.sentiment.polarity
    if sentiment_polarity < 0:
        sentiment = Sentiments.NEGATIVE
    elif sentiment_polarity <= 0.2:
                sentiment = Sentiments.NEUTRAL
    else:
        sentiment = Sentiments.POSITIVE
    tweet['sentiments'] = sentiment
    
def get_tweet(doc):
    tweet = {}
    tweet[id_field] = doc[id_field]
    tweet['hashtags'] = map(lambda x: x['text'],doc['entities']['hashtags'])
    tweet['coordinates'] = doc['coordinates']
    tweet['timestamp_ms'] = doc['timestamp_ms'] 
    tweet['date'] = time.strftime('%Y-%m-%dT%H:%M:%S+00:00', time.strptime(doc['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
    tweet['text'] = doc['text']
    tweet['language'] = doc['lang']
    tweet['source'] = doc['source']
    tweet['user'] = {'id': doc['user']['id'], 'name': doc['user']['name'],
                     'followers': doc['user']['followers_count'], 'friends': doc['user']['friends_count'],
                     'favourites': doc['user']['favourites_count'], 'statuses': doc['user']['statuses_count'],
                     'created_at': time.strftime('%Y-%m-%dT%H:%M:%S+00:00', time.strptime(doc['user']['created_at'],'%a %b %d %H:%M:%S +0000 %Y')), 
                     'time_zone': doc['user']['time_zone'], 'verified': doc['user']['verified']}
    tweet['mentions'] = map(lambda x: x['screen_name'],doc['entities']['user_mentions'])
    _sentiment_analysis(tweet)
    return tweet