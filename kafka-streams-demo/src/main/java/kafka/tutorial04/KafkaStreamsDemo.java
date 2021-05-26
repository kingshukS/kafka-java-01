package kafka.tutorial04;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class KafkaStreamsDemo {

    // consumer details constants
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String APP_ID = "kafka-streams-demo";
    private static final String TOPIC = "twitter_tweets_streams";

    public static void main(String[] args) {
        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,APP_ID);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create topology below in multiple steps

        // Stream builder
        StreamsBuilder builder = new StreamsBuilder();

        // input topic stream
        KStream<String,String> inputTopicStream = builder.stream(TOPIC);
        // filter stream
        KStream<String,String> filteredStream =  inputTopicStream.filter(
                (k,jsonTweet)->extractFollowerForTweetUser(jsonTweet)>10000
        );
        // output topic
        filteredStream.to("important_tweets");

        // build topology
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(properties),properties);

        // start the app
        kafkaStreams.start();
    }

    private static Integer extractFollowerForTweetUser(String jsonTweet) {
        return JsonParser.parseString(jsonTweet).getAsJsonObject().get("user").getAsJsonObject().get("followers_count").getAsInt();
    }
}
