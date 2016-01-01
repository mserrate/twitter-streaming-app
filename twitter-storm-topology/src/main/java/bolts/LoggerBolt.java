package bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mserrate on 14/12/15.
 */
public class LoggerBolt extends BaseBasicBolt {
    public static final Logger LOG = LoggerFactory.getLogger(LoggerBolt.class);
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        LOG.info(tuple.toString());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
}
