package bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by mserrate on 31/12/15.
 */
public class TopHashtagToCassandraBolt extends CassandraBaseBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TopHashtagToCassandraBolt.class);

    public TopHashtagToCassandraBolt(Properties properties) {
        super(properties);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        List<List> rankings = (List) tuple.getValue(0);

        Map<String, Long> rankingMap = new HashMap<>();

        for (List list : rankings) {
            rankingMap.put((String) list.get(0), (Long) list.get(1));
        }

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

        Statement statement = QueryBuilder.insertInto("top_hashtag_by_day")
                .value("date", df.format(new Date()))
                .value("bucket_time", QueryBuilder.raw("dateof(now())"))
                .value("ranking", rankingMap);

        LOG.debug(statement.toString());

        session.execute(statement);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
