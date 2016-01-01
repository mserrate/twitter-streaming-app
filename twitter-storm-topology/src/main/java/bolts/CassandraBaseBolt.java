package bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.util.Map;
import java.util.Properties;

/**
 * Created by mserrate on 28/12/15.
 */
public abstract class CassandraBaseBolt extends BaseBasicBolt {
    private Cluster cluster;
    protected Session session;
    private Properties properties;

    public CassandraBaseBolt(Properties properties) {
        this.properties = properties;
    }

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
}
