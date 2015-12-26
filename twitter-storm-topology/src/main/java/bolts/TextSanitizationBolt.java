package bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.parsing.combinator.testing.Str;
import utils.OfflineReverseGeocode.src.main.java.geocode.GeoName;
import utils.OfflineReverseGeocode.src.main.java.geocode.ReverseGeoCode;

import java.io.IOException;
import java.text.Normalizer;
import java.util.ArrayList;

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

        ArrayList<Double> coordinates = (ArrayList<Double>)tuple.getValue(2);

        String country = "Unknown";
        try {
            GeoName geoName = ReverseGeoCode.getInstance().nearestPlace(coordinates.get(0), coordinates.get(1));
            country = geoName.country;
        } catch (IOException e) {
            LOG.error("Error loading city file: " + e.getMessage());
        }


        collector.emit(new Values(tuple.getLongByField("tweet_id"), text, country, tuple.getStringByField("tweet_created_at")));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet_id", "tweet_text", "tweet_country", "tweet_created_at"));
    }
}
