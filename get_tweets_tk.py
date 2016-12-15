#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, API
import config
from telegram import Bot
import json
import twitter_to_es
import threading
import logging

# FORMAT = '%(asctime)s [%(levelname)s] (%(threadName)-10s) %(message)s'

FORMAT = '%(message)s'
logging.basicConfig(level=logging.CRITICAL, format=FORMAT)
file_handler = logging.FileHandler('teamkeywords.json')
file_handler.setFormatter(logging.Formatter(FORMAT))
logging.getLogger().addHandler(file_handler)

consumer_key = config.consumer_key
consumer_secret = config.consumer_secret
access_token = config.access_token
access_secret = config.access_secret
telegram_token = config.telegram_token
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = API(auth)

twitter_to_es.check_index()


def send_to_es(line):
    doc = json.loads(line)
    if "limit" not in doc:
        twitter_to_es.load(doc)


def write_json(data):
    logging.critical(data)


class MyListener(StreamListener):

    def on_data(self, data):
        try:
            threading.Thread(target=write_json, name="JSON-WRITER",
                             args=(data.rstrip('\n'),)).start()
            threading.Thread(target=send_to_es, name="ES-LOADER",
                             args=(data,)).start()
            return True
        except BaseException as e:
            print("Error on: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


try:
    twitter_stream = Stream(auth, MyListener())
    twitter_stream.filter(track=["adidas", "nike", "messi", "ronaldo", "cristianoronaldo", "cristiano ronaldo", "lionelmessi", "lionel messi", "adidasfootball", "nikefootball", "firstneverfollows", "justdoit", "adidassoccer", "nikesoccer", "fcbarcelona", "realmadrid"])
except:
    chat_id = 27002116
    bot = Bot(token=telegram_token)
    text = "Error, forever will restart the streaming script"
    bot.sendMessage(chat_id=chat_id, text=text)
