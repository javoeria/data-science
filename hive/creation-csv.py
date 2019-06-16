#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 14 10:17:49 2019

@author: benji
"""

from datetime import datetime
from pymongo import MongoClient

connection = MongoClient('localhost', 27017)
db = connection.twitter
collection = db.tweets
print(collection.count())

file = open("tweets.csv",'w')

for item in collection.find():
  try:
	  id_str = item['id_str']
	  text = item['text'].strip()
	  if "," in text:
	      continue
	  username = item['user']['screen_name']
	  retweet_count = int(item['retweet_count'])
	  favorite_count = int(item['favorite_count'])
	  created_at = item['created_at']

	  user_id = item['user']['id_str']
	  name = item['user']['name']
	  description = item['user']['description']
	  statuses_count = int(item['user']['statuses_count'])
	  followers_count = int(item['user']['followers_count'])
	  location = item['user']['location']
	  lang = item['lang']
	  source = item['source']
	  
	  line = str(id_str)+","+text+","+username+","+str(retweet_count)+","+str(favorite_count)+","+created_at+","+str(user_id)+","+name+","+username+","+description+","+str(statuses_count)+","+str(followers_count)+","+location+","+lang+","+source+"\n"
	  print(line)
	  file.write(line)
  except UnicodeEncodeError:
      continue
  
file.close()