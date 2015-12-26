package bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.parsing.combinator.testing.Str;

import java.util.ArrayList;

/**
 * Created by mserrate on 25/12/15.
 */
public class TwitterFilterBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TwitterFilterBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            JSONObject object = (JSONObject)JSONValue.parseWithException(tuple.getString(0));

            if (object.containsKey("lang") && "en".equals(object.get("lang"))) {
                long id = (long)object.get("id");
                String text = (String)object.get("text");
                String created_at = (String)object.get("created_at");

                ArrayList<Double> coordinates = new ArrayList<>();
                if (object.containsKey("coordinates")) {
                    JSONObject coordinatesObject = (JSONObject)object.get("coordinates");
                    JSONArray coordinatesArray = (JSONArray)coordinatesObject.get("coordinates");
                    coordinates.add((Double)coordinatesArray.get(0));
                    coordinates.add((Double)coordinatesArray.get(1));
                }

                collector.emit(new Values(id, text, coordinates, created_at));
            }
            else {
                LOG.debug("Ignoring non-english tweets");
            }

        } catch (ParseException e) {
            LOG.error("Error parsing tweet: " + e.getMessage());
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet_id", "tweet_text", "tweet_coordinates", "tweet_created_at"));
    }
}
