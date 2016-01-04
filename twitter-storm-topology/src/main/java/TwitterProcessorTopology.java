import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.*;
import storm.kafka.*;
import storm.kafka.bolt.KafkaBolt;

import java.util.Properties;

/**
 * Created by mserrate on 14/12/15.
 */
public class TwitterProcessorTopology extends BaseTopology {

    public TwitterProcessorTopology(String configFileLocation) throws Exception {
        super(configFileLocation);
    }

    private void configureKafkaSpout(TopologyBuilder topology) {
        BrokerHosts hosts = new ZkHosts(topologyConfig.getProperty("zookeeper.host"));

        SpoutConfig spoutConfig = new SpoutConfig(
                hosts,
                topologyConfig.getProperty("kafka.twitter.raw.topic"),
                topologyConfig.getProperty("kafka.zkRoot"),
                topologyConfig.getProperty("kafka.consumer.group"));
        spoutConfig.scheme= new SchemeAsMultiScheme(new StringScheme());

        KafkaSpout kafkaSpout= new KafkaSpout(spoutConfig);
        topology.setSpout("twitterSpout", kafkaSpout);
    }

    private void configureBolts(TopologyBuilder topology) {
        // filtering
        topology.setBolt("twitterFilter", new TwitterFilterBolt(), 4)
                .shuffleGrouping("twitterSpout");

        // sanitization
        topology.setBolt("textSanitization", new TextSanitizationBolt(), 4)
                .shuffleGrouping("twitterFilter");

        // sentiment analysis
        topology.setBolt("sentimentAnalysis", new SentimentAnalysisBolt(), 4)
                .shuffleGrouping("textSanitization");

        // persist tweets with analysis to Cassandra
        topology.setBolt("sentimentAnalysisToCassandra", new SentimentAnalysisToCassandraBolt(topologyConfig), 4)
                .shuffleGrouping("sentimentAnalysis");

        // divide sentiment by hashtag
        topology.setBolt("hashtagSplitter", new HashtagSplitterBolt(), 4)
                .shuffleGrouping("textSanitization");

        // persist hashtags to Cassandra
        topology.setBolt("hashtagCounter", new HashtagCounterBolt(), 4)
                .fieldsGrouping("hashtagSplitter", new Fields("tweet_hashtag"));

        topology.setBolt("topHashtag", new TopHashtagBolt())
                .globalGrouping("hashtagCounter");

        topology.setBolt("topHashtagToCassandra", new TopHashtagToCassandraBolt(topologyConfig), 4)
                .shuffleGrouping("topHashtag");

        //topology.setBolt("logBolt", new LoggerBolt()).shuffleGrouping("hashtagSplitter");
    }

    private void buildAndSubmit() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        configureKafkaSpout(builder);
        configureBolts(builder);

        Config config = new Config();

        //set producer properties
        Properties props = new Properties();
        props.put("metadata.broker.list", topologyConfig.getProperty("kafka.broker.list"));
        props.put("request.required.acks", "1");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        config.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);

        StormSubmitter.submitTopology("twitter-processor", config, builder.createTopology());
    }

    public static void main(String[] args) throws Exception {
        String configFileLocation = args[0];

        TwitterProcessorTopology topology = new TwitterProcessorTopology(configFileLocation);
        topology.buildAndSubmit();
    }
}
