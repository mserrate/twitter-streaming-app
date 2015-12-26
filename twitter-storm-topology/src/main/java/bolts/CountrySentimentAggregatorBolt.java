package bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by mserrate on 26/12/15.
 */
public class CountrySentimentAggregatorBolt extends BaseBasicBolt {
    Map<String, Double> sentiment_aggregated = new HashMap<String, Double>();
    Map<String, Integer> count_aggregated = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String country = tuple.getStringByField("tweet_country");
        Double sentiment = tuple.getDoubleByField("tweet_sentiment");
        Double country_sentiment = sentiment_aggregated.get(country);
        Integer country_total = count_aggregated.get(country);

        if (country_sentiment == null)
            country_sentiment = 0.0;

        if (country_total == null)
            country_total = 0;

        country_sentiment += sentiment;
        country_total++;

        sentiment_aggregated.put(country, country_sentiment);
        count_aggregated.put(country, country_total);

        collector.emit(new Values(country, country_sentiment, country_total));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("country", "total_sentiment", "total_count"));
    }
}
