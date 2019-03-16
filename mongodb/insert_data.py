import os
import sys
import dotenv
import pymongo
import twitter
from pymongo import MongoClient

dotenv.load_dotenv()

CONSUMER_KEY       = os.getenv('CONSUMER_KEY')
CONSUMER_SECRET    = os.getenv('CONSUMER_SECRET')
OAUTH_TOKEN        = os.getenv('OAUTH_TOKEN')
OAUTH_TOKEN_SECRET = os.getenv('OAUTH_TOKEN_SECRET')

if len(sys.argv)==1:
  sys.exit('Argument not found')

auth = twitter.oauth.OAuth(OAUTH_TOKEN, OAUTH_TOKEN_SECRET, CONSUMER_KEY, CONSUMER_SECRET)
twitter_api = twitter.Twitter(auth=auth)

connection = MongoClient('localhost', 27017)
db = connection.twitter
collection = db.tweets

search_results = twitter_api.search.tweets(count=100, q=str(sys.argv[1]))
tweets = search_results['statuses']
collection.insert_many(tweets)
print(len(tweets))