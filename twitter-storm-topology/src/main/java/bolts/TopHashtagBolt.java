package bolts;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by mserrate on 30/12/15.
 */
public class TopHashtagBolt extends BaseBasicBolt {
    List<List> rankings = new ArrayList<List>();
    private static final Logger LOG = LoggerFactory.getLogger(TopHashtagBolt.class);
    private static final Integer TOPN = 20;
    private static final Integer TICK_FREQUENCY = 10;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        if (isTickTuple(tuple)) {
            LOG.debug("Tick: " + rankings);
            collector.emit(new Values(new ArrayList(rankings)));
        } else {
            rankHashtag(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tophashtags"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, TICK_FREQUENCY);
        return conf;
    }

    private void rankHashtag(Tuple tuple) {
        String hashtag = tuple.getStringByField("hashtag");
        Integer existingIndex = find(hashtag);
        if (null != existingIndex)
            rankings.set(existingIndex, tuple.getValues());
        else
            rankings.add(tuple.getValues());

        Collections.sort(rankings, new Comparator<List>() {
            @Override
            public int compare(List o1, List o2) {
                return compareRanking(o1, o2);
            }
        });

        shrinkRanking();
    }

    private Integer find(String hashtag) {
        for(int i = 0; i < rankings.size(); ++i) {
            String current = (String) rankings.get(i).get(0);
            if (current.equals(hashtag)) {
                return i;
            }
        }
        return null;
    }

    private int compareRanking(List one, List two) {
        long valueOne = (Long) one.get(1);
        long valueTwo = (Long) two.get(1);
        long delta = valueTwo - valueOne;
        if(delta > 0) {
            return 1;
        } else if (delta < 0) {
            return -1;
        } else {
            return 0;
        }
    }

    private void shrinkRanking() {
        int size = rankings.size();
        if (TOPN >= size) return;
        for (int i = TOPN; i < size; i++) {
            rankings.remove(rankings.size() - 1);
        }
    }

    private static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }
}
