package bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by mserrate on 26/12/15.
 */
public class HashtagSentimentCounterBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(HashtagSentimentCounterBolt.class);
    private Map<String, Double> sentiment_aggregated = new HashMap<String, Double>();
    private Map<String, Long> hashtag_count = new HashMap<String, Long>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String hashtag = tuple.getStringByField("tweet_hashtag");
        Double sentiment = tuple.getDoubleByField("tweet_sentiment");
        Long count = hashtag_count.get(hashtag);
        Double hashtag_sentiment = sentiment_aggregated.get(hashtag);

        if (count == null)
            count = 0L;

        if (hashtag_sentiment == null)
            hashtag_sentiment = 0.0;

        count++;
        hashtag_count.put(hashtag, count);
        hashtag_sentiment += sentiment;
        sentiment_aggregated.put(hashtag, hashtag_sentiment);

        collector.emit(new Values(hashtag, count, hashtag_sentiment));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag", "count", "sentiment"));
    }
}
