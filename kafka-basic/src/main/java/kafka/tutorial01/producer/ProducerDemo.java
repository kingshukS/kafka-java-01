package kafka.tutorial01.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    private static final String BOOTSTRAP_SERVER = "localhost:9092";


    public static void main(String[] args) {
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);

        // create ProducerRecord to send
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic","Hello World from Java Producer");

        // send data
        kafkaProducer.send(producerRecord);

        //flush data
        kafkaProducer.flush();

        //close producer
        kafkaProducer.close();
    }
}
