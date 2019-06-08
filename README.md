# Data Science

Data analysis of the social network Twitter with NoSQL databases

To get started, install the necessary libraries

`pip install -r requirements.txt`

### MongoDB

Import data to the database **twitter** and collection **tweets**

`mongorestore -d twitter -c tweets tweets.bson`

Optional, insert new tweets using Twitter API

`python insert_data.py 1000`

### Cassandra

Create a keyspace with name **twitter** using Cqlsh

`CREATE KEYSPACE twitter WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };`

Import data to the keyspace through MongoDB collection

`python import_data.py`

### Neo4j

Create a local graph using Neo4j Desktop

Import data to the graph through MongoDB collection

`python import_data.py`