

### Compile twitter to kafka producer
```
cd twitter-kafka-producer
mvn assembly:assembly
```
### Compile Storm topology
```
cd twitter-storm-topology
mvn assembly:assembly
```
### Execute docker image (check https://github.com/mserrate/CoreOS-BigData for more info)
```
docker run --rm -ti -v /home/core/share:/root/share -e BROKER_LIST=`fleetctl list-machines -no-legend=true -fields=ip | sed 's/$/:9092/' | paste -s -d ','` -e NIMBUS_HOST=`etcdctl get /storm-nimbus` -e ZK=`fleetctl list-machines -no-legend=true -fields=ip | paste -s -d ','` mserrate/devel-env start-shell.sh bash
```
### Create Kafka topic
```
$KAFKA_HOME/bin/kafka-topics.sh --create --topic twitter-raw-topic --partitions 3 --zookeeper $ZK --replication-factor 2
$KAFKA_HOME/bin/kafka-topics.sh --describe --topic twitter-raw-topic --zookeeper $ZK
```
### Read from kafka topic
```
$KAFKA_HOME/bin/kafka-console-consumer.sh -zookeeper $ZK --topic twitter-raw-topic --from-beginning
```


### Create Cassandra keyspace and table
```
echo "CREATE KEYSPACE stormtwitter WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };" | cqlsh 172.17.8.101
echo "CREATE TABLE IF NOT EXISTS stormtwitter.tweet_sentiment_analysis ( tweet_id bigint, created_at text, tweet text, sentiment double, hashtags set<text>, PRIMARY KEY (tweet_id) );" | cqlsh 172.17.8.101 
echo "CREATE INDEX ON stormtwitter.tweet_sentiment_analysis (hashtags);" | cqlsh 172.17.8.101
echo "CREATE TABLE IF NOT EXISTS stormtwitter.top_hashtag_by_day ( date text, bucket_time timestamp, ranking list<frozen<tuple<text, bigint, double>>>, PRIMARY KEY (date, bucket_time), ) WITH CLUSTERING ORDER BY (bucket_time DESC);" | cqlsh 172.17.8.101

```

### Execute twitter producer
```
java -jar twitter-kafka-producer-1.0-SNAPSHOT-jar-with-dependencies.jar conf/config.properties
```

### Submit storm topology
```
storm jar twitter-storm-topology-1.0-SNAPSHOT-jar-with-dependencies.jar TwitterProcessorTopology conf/config.properties
```