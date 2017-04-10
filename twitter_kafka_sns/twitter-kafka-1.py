#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr  8 19:05:15 2017

@author: Mohneesh
"""


import boto3
import time
from twython import Twython
from datetime import datetime
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

#Access_Keys to be obtained from Twitter Application
#Used to make Twitter API request

TWITTER_ACCESS_TOKEN = '598322060-85YdYXf17Ecf30GXZdT7hNqypapNUfIbk6JzCHxv'
TWITTER_ACCESS_TOKEN_SECRET = 'M6M1MLGQCNm8Rfb3wUOMx5h9b6i0LJuA25DbDgFMhlIwk'

#Consumer keys to access Twitter Application 

TWITTER_APP_KEY = 'pDLnK0jtrrj3TOlj9eKr7yOQt'
TWITTER_APP_KEY_SECRET = '6rZ2NeZq7MjC0lQVA501zAtuBso845njET2TTReENJRrmJTbet'

#Authenticating Credentials 

twitterauth = Twython(app_key=TWITTER_APP_KEY,
            app_secret=TWITTER_APP_KEY_SECRET,
            oauth_token=TWITTER_ACCESS_TOKEN,
            oauth_token_secret=TWITTER_ACCESS_TOKEN_SECRET)

# Initializing Kafka

KAFKA_HOST = 'localhost:9092'
TOPIC = 'test'

producer = KafkaProducer(bootstrap_servers=[KAFKA_HOST])

def get_tweets(keyword):
    
    search = twitterauth.search(q=keyword,count=100)
    tweets = []
    tweets = search['statuses']
    for tweet in tweets:

        if tweet['geo'] != None:
            
            print (tweet['user']['lang'])
            if tweet['user']['lang']=='en':               
                text = tweet['text'].lower().encode('ascii','ignore').decode('ascii')                
                index = tweet['id']                
                coordinates = tweet['geo']['coordinates']
                
                message={
                'id':index,
                'text':text,
                'coordinates':coordinates,
                'sentiment':''
                }
                temp=json.dumps(message).encode('utf-8')
                print (temp)
                response = producer.send(TOPIC,temp)




def twittmap():
    try:
        for i in range(1,40):
            get_tweets('movies')
            time.sleep(5)
            get_tweets('technology')
            time.sleep(5)
            get_tweets('sports')
            time.sleep(5)
            get_tweets('life')
            time.sleep(5)
            get_tweets('news')
            time.sleep(5)
            get_tweets('travel')
            time.sleep(5)
            get_tweets('health')
            time.sleep(5)
            get_tweets('awesome')
            time.sleep(5)
            get_tweets('energy')
            time.sleep(5)
            get_tweets('music')
            time.sleep(5)
    except Exception as e:
        print(str(e)) 
        
        #pass
        return

if __name__ == '__main__':
    while True:
        twittmap()
        