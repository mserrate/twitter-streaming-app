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
public class TwitterProcessorTopology {
    private Context context;

    public TwitterProcessorTopology(Context context) {
        this.context = context;
    }

    private void configureKafkaSpout(TopologyBuilder topology) {
        BrokerHosts hosts = new ZkHosts(context.getString("zookeeper.host"));

        SpoutConfig spoutConfig = new SpoutConfig(
                hosts,
                context.getString("kafka.twitter.raw.topic"),
                context.getString("kafka.zkRoot"),
                context.getString("kafka.consumer.group"));
        spoutConfig.scheme= new SchemeAsMultiScheme(new StringScheme());

        KafkaSpout kafkaSpout= new KafkaSpout(spoutConfig);
        topology.setSpout("twitterSpout", kafkaSpout);
    }

    private void configureBolts(TopologyBuilder topology) {
        //topology.setBolt("logBolt", new PrinterBolt()).shuffleGrouping("twitterSpout");
        topology.setBolt("twitterFilter", new TwitterFilterBolt(), 4)
                .shuffleGrouping("twitterSpout");

        // sanitization
        topology.setBolt("textSanitization", new TextSanitizationBolt(), 4)
                .shuffleGrouping("twitterFilter");

        // sentiment analysis
        topology.setBolt("sentimentAnalysis", new SentimentAnalysisBolt(), 4)
                .shuffleGrouping("textSanitization");

        // persist tweets with analysis to Cassandra
        topology.setBolt("sentimentAnalysisToCassandra", new SentimentAnalysisToCassandraBolt(), 4)
                .shuffleGrouping("sentimentAnalysis");

        // aggregate sentiments per country
        topology.setBolt("countryAggregator", new CountrySentimentAggregatorBolt(), 4)
                .fieldsGrouping("sentimentAnalysis", new Fields("tweet_country"));

        // persist totals to Cassandra
        topology.setBolt("countryAggregateToCassandra", new CountrySentimentAggregatorToCassandraBolt(), 4)
                .shuffleGrouping("countryAggregator");
    }

    private void buildAndSubmit() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        configureKafkaSpout(builder);
        configureBolts(builder);

        Config config = new Config();

        //set producer properties
        Properties props = new Properties();
        props.put("metadata.broker.list", context.getString("kafka.broker.list"));
        props.put("request.required.acks", "1");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        config.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);

        StormSubmitter.submitTopology("twitter-processor", config, builder.createTopology());
    }

    public static void main(String[] args) throws Exception {
        String configFileLocation = args[0];

        TwitterProcessorTopology topology = new TwitterProcessorTopology(new Context(configFileLocation));
        topology.buildAndSubmit();
    }
}
