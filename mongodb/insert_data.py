import os
import sys
import dotenv
import pymongo
from twitter import *
from pymongo import MongoClient
from faker import Faker

dotenv.load_dotenv()

CONSUMER_KEY       = os.getenv('CONSUMER_KEY')
CONSUMER_SECRET    = os.getenv('CONSUMER_SECRET')
OAUTH_TOKEN        = os.getenv('OAUTH_TOKEN')
OAUTH_TOKEN_SECRET = os.getenv('OAUTH_TOKEN_SECRET')

if len(sys.argv)==1:
  sys.exit('Argument not found')

connection = MongoClient('localhost', 27017)
db = connection.test
collection = db.tweets
collection.create_index([('text', pymongo.TEXT)])
collection.create_index([('geo', pymongo.GEOSPHERE)])

auth = OAuth(OAUTH_TOKEN, OAUTH_TOKEN_SECRET, CONSUMER_KEY, CONSUMER_SECRET)
twitter = Twitter(auth = auth)
fake = Faker()
total = 0

while total < int(sys.argv[1]):
  word = fake.word()
  query = twitter.search.tweets(result_type = 'popular', count = 100, q = word)
  tweets = query['statuses']
  if len(tweets) > 0:
    collection.insert_many(tweets)
    total += len(tweets)
    print(word, total)
