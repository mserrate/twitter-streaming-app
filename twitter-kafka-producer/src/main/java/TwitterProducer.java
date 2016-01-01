import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by mserrate on 14/12/15.
 */
public class TwitterProducer {
    private static final String BROKER_LIST = "kafka.broker.list";
    private static final String CONSUMER_KEY = "consumerKey";
    private static final String CONSUMER_SECRET = "consumerSecret";
    private static final String TOKEN = "accessToken";
    private static final String SECRET = "accessTokenSecret";
    private static final String KAFKA_TOPIC = "kafka.twitter.raw.topic";

    public static void run(Context context) throws InterruptedException {
        // Producer properties
        Properties properties = new Properties();
        properties.put("metadata.broker.list", context.getString(BROKER_LIST));
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks", "1");
        ProducerConfig producerConfig = new ProducerConfig(properties);

        final Producer<String, String> producer = new Producer<String, String>(producerConfig);


        // Create an appropriately sized blocking queue
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

        // Define our endpoint: By default, delimited=length is set (we need this for our processor)
        // and stall warnings are on.
        StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
        endpoint.stallWarnings(false);

        Authentication auth = new OAuth1(context.getString(CONSUMER_KEY), context.getString(CONSUMER_SECRET), context.getString(TOKEN), context.getString(SECRET));
        //Authentication auth = new com.twitter.hbc.httpclient.auth.BasicAuth(username, password);

        // Create a new BasicClient. By default gzip is enabled.
        BasicClient client = new ClientBuilder()
                .name("twitterClient")
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        // Establish a connection
        client.connect();

        // Do whatever needs to be done with messages
        for (int msgRead = 0; msgRead < 1000; msgRead++) {
            if (client.isDone()) {
                System.out.println("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
                break;
            }

            KeyedMessage<String, String> message = null;
            try {
                message = new KeyedMessage<String, String>(context.getString(KAFKA_TOPIC), queue.take());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            producer.send(message);
        }

        producer.close();
        client.stop();

        // Print some stats
        System.out.printf("The client read %d messages!\n", client.getStatsTracker().getNumMessages());
    }

    public static void main(String[] args) {
        try {
            String configFileLocation = args[0];

            Context context = new Context(configFileLocation);
            TwitterProducer.run(context);
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}

