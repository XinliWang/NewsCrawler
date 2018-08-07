import os
import sys
import datetime
from dateutil import parser
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))

from cloudAMQP_client import CloudAMQPClient
import mongodb_client

AMQP_URL = ''
DEDUPE_NEWS_QUEUE_NAME = 'top-news-dedupe-news-queue'
SLEEP_TIME_OUT_IN_SECONDS = 1
NEWS_TABLE_NAME = 'news'
# Connect dedupe queue
cloudAMQP_dedupe_client = CloudAMQPClient(AMQP_URL, DEDUPE_NEWS_QUEUE_NAME)
# Connect Mongo DB
db = mongodb_client.get_db()

# start to work
while True:
    # Get message from dedupe queue
    message = cloudAMQP_dedupe_client.getMessage()

    # handle the message
    if message is not None:
        try:
            handle_message(message)
        except Exception as e:
            print e
            pass

    cloudAMQP_dedupe_client.sleep(SLEEP_TIME_OUT_IN_SECONDS)


def handle_message(message):
    if message is None or not isinstance(message, dict):
        print 'message is broken'
        return
    news_content = str(message['content'])

    # get the published day of this message and get one day's data from DB
    published_at = paser.parse(message['publishedAt'])
    published_at_day_begin = datetime.datetime(published_at.year, published_at.month, published_at.day, 0, 0, 0)
    published_at_day_end = published_at_day_begin + datetime.timedelta(days=1)

    recent_news_list = db[NEWS_TABLE_NAME].find({'publishedAt': {'$gte': published_at_day_begin, '$lt': published_at_day_end}})
    # compare this message with data in one day to get the similarity
    if recent_news_list is not None and len(recent_news_list) > 0:
        # TODO: check similarity use sklearn
    # if not similar, store it into DB
