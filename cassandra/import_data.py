import json
from datetime import datetime
from pymongo import MongoClient
from cassandra.cluster import Cluster

connection = MongoClient('localhost', 27017)
db = connection.twitter
collection = db.tweets
print(collection.count())

cluster = Cluster()
session = cluster.connect()
session.set_keyspace('twitter')


session.execute("CREATE TABLE twitter.tweets (id text, text text, username text, retweet_count int, favorite_count int, created_at timestamp, PRIMARY KEY (id))")
session.execute("CREATE TABLE twitter.tweets_by_lang (lang text, count int, PRIMARY KEY (lang))")
session.execute("CREATE TABLE twitter.tweets_by_hour (hour int, count int, PRIMARY KEY (hour))")
session.execute("CREATE TABLE twitter.tweets_by_source (source text, count int, PRIMARY KEY (source))")
session.execute("CREATE TABLE twitter.tweets_coordinates (id text, place text, coordinate_x double, coordinate_y double, PRIMARY KEY (id))")
session.execute("CREATE TABLE twitter.users (id text, name text,	username text, description text, statuses_count int, followers_count int,	PRIMARY KEY (id))")
session.execute("CREATE TABLE twitter.users_by_location (location text, count int,	PRIMARY KEY (location))")
session.execute("CREATE TABLE twitter.trending_topics (hashtag text, count int, PRIMARY KEY (hashtag))")


for item in collection.find():
  id = item['id_str']
  text = item['text']
  username = item['user']['screen_name']
  retweet_count = int(item['retweet_count'])
  favorite_count = int(item['favorite_count'])
  created_at = datetime.strptime(item['created_at'], '%a %b %d %H:%M:%S %z %Y') # Fri May 01 15:43:55 +0000 2015
  session.execute("INSERT INTO tweets (id,text,username,retweet_count,favorite_count,created_at) VALUES (%s,%s,%s,%s,%s,%s)", (id, text, username, retweet_count, favorite_count, created_at)) 

  user_id = item['user']['id_str']
  name = item['user']['name']
  description = item['user']['description']
  statuses_count = int(item['user']['statuses_count'])
  followers_count = int(item['user']['followers_count'])
  session.execute("INSERT INTO users (id,name,username,description,statuses_count,followers_count) VALUES (%s,%s,%s,%s,%s,%s)", (user_id, name, username, description, statuses_count, followers_count)) 


tweets_by_lang = [
  {'$group': {'_id': '$lang', 'count': {'$sum': 1}}}
]
for item in collection.aggregate(tweets_by_lang):
  session.execute("INSERT INTO tweets_by_lang (lang,count) VALUES (%s,%s)", (item['_id'], item['count'])) 


tweets_by_hour = [
  {'$group': 
    {
      '_id': {'$hour': {'$dateFromString': {'dateString': '$created_at'}}},
      'count': {'$sum': 1}
    }
  }
]
data = {'_id': range(24), 'count': [0]*24}
for hour in collection.aggregate(tweets_by_hour):
  num = hour['_id']
  data['count'][num] = hour['count']
count = 0
for item in data['count']:
  session.execute("INSERT INTO tweets_by_hour (hour,count) VALUES (%s,%s)", (count, item))
  count += 1


tweets_by_source = [
  {'$group': 
    {
      '_id': {'$arrayElemAt': [{'$split': [{'$arrayElemAt': [{'$split': ['$source', '>']}, 1]}, '<']}, 0]},
      'count': {'$sum': 1}
    }
  }
]
for item in collection.aggregate(tweets_by_source):
  session.execute("INSERT INTO tweets_by_source (source,count) VALUES (%s,%s)", (item['_id'], item['count'])) 


users_by_location = [
  {'$group': {'_id': '$user.location', 'count': {'$sum': 1}}}
]
for item in collection.aggregate(users_by_location):
  if item['_id'] != '':
    session.execute("INSERT INTO users_by_location (location,count) VALUES (%s,%s)", (item['_id'], item['count'])) 


trending_topics = [
  {'$project':
    {'hashtags':
      {'$map': {
        'input': '$entities.hashtags',
        'as': 'entities',
        'in': {'$concat': ['#', '$$entities.text']}
      }},
    }
  },
  {'$unwind': '$hashtags'},
  {'$group': {'_id': '$hashtags', 'count': {'$sum': 1}}}
]
for item in collection.aggregate(trending_topics):
  session.execute("INSERT INTO trending_topics (hashtag,count) VALUES (%s,%s)", (item['_id'], item['count'])) 


for item in collection.find({'geo': {'$ne': None}}):
  id = item['id_str']
  place = item['place']['full_name']
  coordinate_x = item['geo']['coordinates'][0]
  coordinate_y = item['geo']['coordinates'][1]
  session.execute("INSERT INTO tweets_coordinates (id,place,coordinate_x,coordinate_y) VALUES (%s,%s,%s,%s)", (id, place, coordinate_x, coordinate_y)) 

