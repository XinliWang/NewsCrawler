import requests
from json import loads
import yaml

with open("../config.yml", 'r') as ymlfile:
    config = yaml.load(ymlfile)

NEWS_API_ENGPOINT = 'https://newsapi.org/v2/'
NEWS_API_SOURCE = 'top-headlines'
API_KEY = config['NEWS_API_KEY']
NYTIMES = 'the-new-york-times'
DEFAULT_SOURCES = [NYTIMES]

def getURL(endpoint=NEWS_API_ENGPOINT, api_name=NEWS_API_SOURCE):
    return endpoint + api_name

def getNewsFromSource(sources=DEFAULT_SOURCES):
    articles = []
    for source in sources:
        payload = {
            "apiKey" : API_KEY,
            "sources" : source
        }
        response = requests.get(getURL(), params=payload)
        res_json = loads(response.content)
        if (res_json is not None and
            res_json['status'] == "ok" and
            res_json['totalResults'] > 0):

            articles.extend(res_json['articles'])
    return articles
