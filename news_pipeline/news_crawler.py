import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from cloudAMQP_client import CloudAMQPClient
import requests
from goose import Goose

# AMQP_URL = ''
# SCRAPE_NEWS_QUEUE_NAME = 'top-news-scrape-news-queue'
# DEDUPE_NEWS_QUEUE_NAME = 'top-news-dedupe-news-queue'
# SLEEP_TIME_OUT_IN_SECONDS = 5

# # Connect two message queues
# cloudAMQP_scrape_client = CloudAMQPClient(AMQP_URL, SCRAPE_NEWS_QUEUE_NAME)
# cloudAMQP_dedupe_client = CloudAMQPClient(AMQP_URL, DEDUPLICATION_NEWS_QUEUE_NAME)

# while True:
#     # Get message from SCRAPE QUEUE
#     scrape_message = cloudAMQP_scrape_client.getMessage()

#     if scrape_message is not None:
#         # Handle message
#         try:
#             handle_message(scrape_message)
#         except Exception as e:
#             print e
#             pass
#     # sleep
#     cloudAMQP_scrape_client.sleep(SLEEP_TIME_OUT_IN_SECONDS)

def handle_message(message):
    if message is None or not isinstance(message, dict):
        print 'message is broken'
        return
    # Crawl news based on the url which comes from message
    news_content = crawl(message['url'])
    message['content'] = news_content
    # Send the news content into Deduplication QUEUE
    cloudAMQP_dedupe_client.sendMessage(message)

def crawl(url):
    # TODO: crawl the content based on url
    response = requests.get(url)
    extractor = Goose()
    article = extractor.extract(raw_html=response.content)
    text = article.cleaned_text
    print(text)
    return ""

crawl("https://www.wcvb.com/article/5-investigates-mbta-will-close-alewife-garage-temporarily-for-repairs/22699725")