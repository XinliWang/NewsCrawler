# -*- coding: utf-8 -*-
import redis
import os
import sys
import hashlib
import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from cloudAMQP_client import CloudAMQPClient
import news_api_client

REDIS_HOST = 'localhost'
REDIS_PORT = 6378
AMQP_URL = ''
SCRAPE_NEWS_QUEUE_NAME = 'top-news-scrape-news-queue'

NEWS_TIME_OUT_IN_SECONDS = 3600 * 24
SLEEP_TIME_OUT_IN_SECONDS = 10

NEWS_SOURCE = [
    'the-new-york-times'
]

# Connect redis
redis_client = redis.Redis(host = REDIS_HOST,
                           port = REDIS_PORT)

# Connect CloudAMQP
cloudAMQP_client = CloudAMQPClient(AMQP_URL, SCRAPE_NEWS_QUEUE_NAME)

while True:
    # GET NEWS API 的 news
    articles = news_api_client.getNewsFromSource(NEWS_SOURCE)
    # count how many new news need to be saved
    num_of_new_news = 0
    for article in articles:
        # encoding title, store it into redis as the key
        article_digest = hashlib.md5(article['title'].encode('utf-8')).digest().encode('base64')
        # check whether redis already have it, if not, then store it into redis and send it to CloudAMQP QUEUE
        if redis_client.get(article_digest) is None:
            num_of_new_news = num_of_new_news + 1
            article['digest'] = article_digest
            # in case there is no this field, then add current time
            if article['publishedAt'] is None:
                article['publishedAt'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            # insert into redis and set expiration time
            redis_client.set(article_digest, article)
            redis_client.expire(article_digest, NEWS_TIME_OUT_IN_SECONDS)
            # send this new news into Message Queue
            cloudAMQP_client.sendMessage(article)

    print "Fetched %d new news." % num_of_new_news
    # sleep a constant time and go to the next news API call
    cloudAMQP_client.sleep(SLEEP_TIME_OUT_IN_SECONDS)
