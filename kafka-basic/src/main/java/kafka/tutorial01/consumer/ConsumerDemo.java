package kafka.tutorial01.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String GROUP_ID = "my-sixth-app";
    private static final String TOPIC = "second-topic";
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        // create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,GROUP_ID);

        // create the consumer
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        // subscribe to topic(s)
        kafkaConsumer.subscribe(Arrays.asList(TOPIC));

        // poll to fetch data
        while(true){
            ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String,String> record : consumerRecords){
                logger.info(">>Key: "+record.key()+" , Value: "+record.value()
                +" , Topic: "+record.topic()+" , Partition: "+record.partition()
                +" , Offset: "+record.offset());
            }
        }
    }
}
