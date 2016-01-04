package bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.text.Normalizer;

/**
 * Created by mserrate on 25/12/15.
 */
public class TextSanitizationBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TextSanitizationBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

        String text = tuple.getString(1);
        String normalizedText = Normalizer.normalize(text, Normalizer.Form.NFD);
        text = normalizedText.replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
        text = text.replaceAll("[^\\p{L}\\p{Nd}]+", " ").toLowerCase();

        collector.emit(new Values(
                tuple.getLongByField("tweet_id"),
                text,
                tuple.getValueByField("tweet_hashtags"),
                tuple.getStringByField("tweet_created_at")));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet_id", "tweet_text", "tweet_hashtags", "tweet_created_at"));
    }
}
