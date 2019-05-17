# Data Science

### MongoDB

`mongorestore -d twitter -c tweets tweets.bson`

`pip install -r requirements.txt`

`python insert_data.py 1000`

### Cassandra

`CREATE KEYSPACE twitter WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };`

`python import_data.py`
