package bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * Created by mserrate on 26/12/15.
 */
public class SentimentAnalysisToCassandraBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SentimentAnalysisToCassandraBolt.class);

    private Cluster cluster;
    private Session session;
    private Properties properties;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        cluster = Cluster.builder().addContactPoint(properties.getProperty("cassandra.host")).build();
        session = cluster.connect(properties.getProperty("cassandra.keyspace"));
    }

    @Override
    public void cleanup() {
        session.close();
        cluster.close();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

        Statement statement = QueryBuilder.update("tweetSentimentAnalysis")
                .with(QueryBuilder.set("tweetText", tuple.getStringByField("tweet_text")))
                .and(QueryBuilder.set("sentiment", tuple.getDoubleByField("tweet_sentiment")))
                .and(QueryBuilder.set("country", tuple.getStringByField("tweet_country")))
                .where(QueryBuilder.eq("id", tuple.getLongByField("tweet_id")))
                .and(QueryBuilder.eq("createdAt", tuple.getDoubleByField("tweet_created_at")));

        LOG.debug(statement.toString());

        session.execute(statement);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
