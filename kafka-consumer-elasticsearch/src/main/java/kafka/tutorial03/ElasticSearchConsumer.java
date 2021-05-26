package kafka.tutorial03;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    // consumer details constants
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String GROUP_ID = "kafka-elasticsearch-consumer-group-for-streams";
    private static final String TOPICS = "twitter_tweets_streams";

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    // credentials for ES
    private static final String HOSTNAME = "kafka-course-8584313666.us-east-1.bonsaisearch.net";
    private static final String USERNAME = "u7mcbayfe8";
    private static final String PASSWORD = "mss6dygnm7";

    @SuppressWarnings( "deprecation" )
    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = createClient();

        // create kafka consumer
        KafkaConsumer<String, String> kafkaConsumer = createConsumer();
        try {
            // poll to fetch data
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : consumerRecords) {

                    // generate id for ES
                    String id = extractIdForTweet(record.value());

                    // insert record data in ES
                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id)//id is the location to store the data, to make it idempotent
                            .source(record.value(), XContentType.JSON);
                    IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
                    logger.info("Index Response stored in ES with id={}", response.getId());
                }
            }
        } finally {
            client.close();
            kafkaConsumer.close();
        }
    }

    // get the twitter feed specific id
    private static String extractIdForTweet(String jsonTweet) {
        return JsonParser.parseString(jsonTweet).getAsJsonObject().get("id_str").getAsString();
    }

    public static KafkaConsumer<String, String> createConsumer() {

        // create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // offset setting and group_id config properties
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        // auto-commit settings
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"2000");

        // create the consumer and subscribe
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(TOPICS));
        return kafkaConsumer;
    }

    public static RestHighLevelClient createClient() {

       /* // for cloud based BONSAI ES
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(USERNAME,PASSWORD));

        RestClientBuilder builder = RestClient.builder(new HttpHost(HOSTNAME,443,"https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);*/

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));

        return client;
    }
}
