twitter-streaming-app
=====================

## Prerequisites
* [Apache Kafka](http://kafka.apache.org/) 0.8+
* [Apache Storm](http://storm.apache.org/) 0.10+
* [Apache Cassandra](http://cassandra.apache.org/) 2.2+
You can use the following repository to get the above infrastructure on docker containers: https://github.com/mserrate/CoreOS-BigData

### Create Kafka topic
```
$KAFKA_HOME/bin/kafka-topics.sh --create --topic twitter-raw-topic --partitions 3 --zookeeper $ZK --replication-factor 2
# Get the created topic
$KAFKA_HOME/bin/kafka-topics.sh --describe --topic twitter-raw-topic --zookeeper $ZK
```

### Create Cassandra keyspace and tables
```
echo "CREATE KEYSPACE stormtwitter WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };" | cqlsh 172.17.8.101
echo "CREATE TABLE IF NOT EXISTS stormtwitter.tweet_sentiment_analysis ( tweet_id bigint, created_at text, tweet text, sentiment double, hashtags set<text>, PRIMARY KEY (tweet_id) );" | cqlsh 172.17.8.101 
echo "CREATE INDEX ON stormtwitter.tweet_sentiment_analysis (hashtags);" | cqlsh 172.17.8.101
echo "CREATE TABLE IF NOT EXISTS stormtwitter.top_hashtag_by_day ( date text, bucket_time timestamp, ranking map<text, bigint>, PRIMARY KEY (date, bucket_time), ) WITH CLUSTERING ORDER BY (bucket_time DESC);" | cqlsh 172.17.8.101

```

## Building the code
For both [twitter-kafka-producer](twitter-kafka-producer) and [twitter-storm-topology](twitter-storm-topology) execute
```
mvn clean package
```


## Running the solution
Configure the properties file [config.properties](conf/config.properties)
```
# Twitter conf
consumerKey=
consumerSecret=
accessToken=
accessTokenSecret=

# Kafka conf
kafka.broker.list=172.17.8.101:9092,172.17.8.102:9092,172.17.8.103:9092
kafka.twitter.raw.topic=twitter-raw-topic

#ZooKeeper Host
zookeeper.host=172.17.8.101:2181,172.17.8.102:2181,172.17.8.103:2181

#Storm conf
#Location in ZK for the Kafka spout to store state.
kafka.zkRoot=/twitter_spout
kafka.consumer.group=storm

#Cassandra Host
cassandra.host=172.17.8.101
cassandra.keyspace=stormtwitter
```


### Run the twitter producer
```
java -jar twitter-kafka-producer-1.0-SNAPSHOT-jar-with-dependencies.jar conf/config.properties
```

### Submit the storm topology
```
storm jar twitter-storm-topology-1.0-SNAPSHOT-jar-with-dependencies.jar TwitterProcessorTopology conf/config.properties
```