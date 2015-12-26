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
public class CountrySentimentAggregatorToCassandraBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(CountrySentimentAggregatorToCassandraBolt.class);

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

        Statement statement = QueryBuilder.update("sentimentCountry")
                .with(QueryBuilder.set("sentiment", tuple.getDoubleByField("total_sentiment")))
                .and(QueryBuilder.set("numberOfTweets", tuple.getIntegerByField("total_count")))
                .where(QueryBuilder.eq("country", tuple.getLongByField("country")));

        LOG.debug(statement.toString());

        session.execute(statement);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
