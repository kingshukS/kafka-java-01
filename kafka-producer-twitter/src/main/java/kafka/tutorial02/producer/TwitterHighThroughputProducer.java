package kafka.tutorial02.producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


public class TwitterHighThroughputProducer {

    private final String consumerKey;
    private final String consumerSecret;
    private final String token;
    private final String secret;
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    // set up some track terms
    private final List<String> terms = Lists.newArrayList("IPL", "bitcoin", "apple", "macbook", "xiaomi", "iphone", "mi 11 ultra");
    private static Logger logger = LoggerFactory.getLogger(TwitterHighThroughputProducer.class);

    public TwitterHighThroughputProducer() {
        consumerKey = "r3s8DUi2ZVpIVoDkZA0cytMPe";
        consumerSecret = "ekEqlG3ohhfxCkod0qpuCuqmVd23pUdad1GQzVRzXC9Rss1kA2";
        token = "1330733994-TC3p7YqEEB8YaZNWmC78PmFkJsS1RCUk9pJ9ck8";
        secret = "GnbsAJxOp17hgcYtnFoDOjVRyyRsJiWRMpIEMB9JfOsSf";
    }

    public static void main(String[] args) {
        new TwitterHighThroughputProducer().run();
    }

    public void run() {
        logger.info("Setup Application");

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // create twitter client
        Client hosebirdClient = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        hosebirdClient.connect();

        // create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.warn("Stopping Twitter Client...");
            hosebirdClient.stop();
            logger.warn("Closing Producer...");
            producer.close();
            logger.warn("Closing the application...");
        }));
        // loop to send data to kafka
        // on a different thread, or multiple different threads....
        int limit = 500;
        while (!hosebirdClient.isDone() && limit > 0) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                hosebirdClient.stop();
            }
            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets_streams", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            logger.error("Unable to store msg={} with error={}", metadata, exception.getLocalizedMessage());
                        }
                    }
                });
            }
            limit--;
        }
        logger.info("End of Application");
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create safe producer properties
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");// for kafka(2.8 for us)>=1.1 5, else 1


        // high throughput properties(at the expense of a bit of latency and CPU usage)

        /* compression algorithm (can be lz4, gzip etc), defaults to none(no compression),consumer automatically knows how to 
        decompress and read data from the topic, doesn't matter how we compress it, it is auto-configured*/
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); // time that kafka waits before sending a batch out
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32 kb batch-size

        // create the producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        return kafkaProducer;
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);

        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;
    }
}
