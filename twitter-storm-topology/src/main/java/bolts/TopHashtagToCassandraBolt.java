package bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
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

        TupleType tupleType = TupleType.of(DataType.text(), DataType.bigint(), DataType.cdouble());
        List<TupleValue> tupleValues = new ArrayList<TupleValue>();
        for (List list : rankings) {
            TupleValue value = tupleType.newValue();
            value.setString(0, (String) list.get(0));
            value.setLong(1, (Long) list.get(1));
            value.setDouble(2, (Double) list.get(2));
            tupleValues.add(value);
        }

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

        Statement statement = QueryBuilder.insertInto("top_hashtag_by_day")
                .value("date", df.format(new Date()))
                .value("bucket_time", QueryBuilder.raw("dateof(now())"))
                .value("ranking", tupleValues);

        LOG.debug(statement.toString());

        session.execute(statement);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
