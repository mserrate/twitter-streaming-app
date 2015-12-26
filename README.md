

Compile
```
mvn assembly:assembly
```



# Create Kafka topic
$KAFKA_HOME/bin/kafka-topics.sh --create --topic twitter-raw-topic --partitions 3 --zookeeper $ZK --replication-factor 2
# Create Cassandra keyspace and table
echo "CREATE KEYSPACE stormtwitter WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };" | cqlsh 172.17.8.101
echo "CREATE TABLE IF NOT EXISTS stormtwitter.tweetSentimentAnalysis ( id bigint, createdAt text, tweetText text, sentiment double, country text, PRIMARY KEY (siteId, createdAt);" | cqlsh 172.17.8.101 
echo "CREATE TABLE IF NOT EXISTS stormtwitter.sentimentCountry ( country text, sentiment double, numberOfTweets int, PRIMARY KEY (country);" | cqlsh 172.17.8.101