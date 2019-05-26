from py2neo import Graph, Node, Relationship
from pymongo import MongoClient
from datetime import datetime

connection = MongoClient('localhost', 27017)
db = connection.twitter
collection = db.tweets
print(collection.count())

graph = Graph(password = 'root')
users_unique = []
sources_unique = []
languages_unique = []
locations_unique = []

tx = graph.begin()
for item in collection.find():
  user_id = item['user']['id_str']
  name = item['user']['name']
  username = item['user']['screen_name']
  description = item['user']['description']
  statuses_count = int(item['user']['statuses_count'])
  followers_count = int(item['user']['followers_count'])
  user = Node('User', id_str=user_id, name=name, username=username, description=description, statuses_count=statuses_count, followers_count=followers_count)
  if username not in users_unique:
    users_unique.append(username)
    tx.create(user)

  id_str = item['id_str']
  text = item['text']
  retweet_count = int(item['retweet_count'])
  favorite_count = int(item['favorite_count'])
  created_at = datetime.strptime(item['created_at'], '%a %b %d %H:%M:%S %z %Y') # Fri May 01 15:43:55 +0000 2015
  if item['geo'] == None:
    place = None
    coordinate_x = None
    coordinate_y = None
  else:
    place = item['place']['full_name']
    coordinate_x = item['geo']['coordinates'][0]
    coordinate_y = item['geo']['coordinates'][1]
  tweet = Node('Tweet', id_str=id_str, text=text, retweet_count=retweet_count, favorite_count=favorite_count, created_at=created_at, place=place, coordinate_x=coordinate_x, coordinate_y=coordinate_y)
  tx.create(tweet)

  src = item['source'].split('>')[1].split('<')[0]
  source = Node('Source', name=src)
  if src not in sources_unique:
    sources_unique.append(src)
    tx.create(source)

  lang = item['lang'].upper()
  language = Node('Language', name=lang)
  if lang not in languages_unique:
    languages_unique.append(lang)
    tx.create(language)

  loc = item['user']['location']
  location = Node('Location', name=loc)
  if loc != '' and loc not in locations_unique:
    locations_unique.append(loc)
    tx.create(location)

  rel_has = Relationship(user, 'HAS', tweet)
  tx.merge(user, 'User', 'id_str')
  tx.merge(tweet, 'Tweet', 'id_str')
  tx.merge(rel_has)

  rel_from = Relationship(tweet, 'FROM', source)
  tx.merge(source, 'Source', 'name')
  tx.merge(rel_from)

  rel_in = Relationship(tweet, 'IN', language)
  tx.merge(language, 'Language', 'name')
  tx.merge(rel_in)

  if loc != '':
    rel_lives = Relationship(tweet, 'LIVES', location)
    tx.merge(location, 'Location', 'name')
    tx.merge(rel_lives)

tx.commit()

for item in collection.find({'in_reply_to_status_id': {'$ne': None}}):
  id_str = item['id_str']
  reply_str = item['in_reply_to_status_id_str']
  tweet_og = graph.run("MATCH (t:Tweet) WHERE t.id_str={x} RETURN t", x=reply_str).evaluate()
  tweet = graph.run("MATCH (t:Tweet) WHERE t.id_str={x} RETURN t", x=id_str).evaluate()
  rel_reply = Relationship(tweet, 'REPLY', tweet_og)
  graph.merge(rel_reply)
