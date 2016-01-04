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

import java.util.*;

/**
 * Created by mserrate on 26/12/15.
 */
public class SentimentAnalysisToCassandraBolt extends CassandraBaseBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SentimentAnalysisToCassandraBolt.class);

    public SentimentAnalysisToCassandraBolt(Properties properties) {
        super(properties);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

        HashSet<String> hashtags = (HashSet<String>)tuple.getValueByField("tweet_hashtags");

        Statement statement = QueryBuilder.update("tweet_sentiment_analysis")
                .with(QueryBuilder.set("tweet", tuple.getStringByField("tweet_text")))
                .and(QueryBuilder.set("sentiment", tuple.getDoubleByField("tweet_sentiment")))
                .and(QueryBuilder.addAll("hashtags", hashtags))
                .and(QueryBuilder.set("created_at", tuple.getStringByField("tweet_created_at")))
                .where(QueryBuilder.eq("tweet_id", tuple.getLongByField("tweet_id")));

        LOG.debug(statement.toString());

        session.execute(statement);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
