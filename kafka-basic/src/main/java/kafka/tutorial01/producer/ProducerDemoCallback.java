package kafka.tutorial01.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoCallback {

    private static final String BOOTSTRAP_SERVER = "localhost:9092";


    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoCallback.class);
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);

        for(int i = 0; i < 10; i++) {

            String topic = "first_topic";
            String value = "Hello Kingshuk : " + Integer.toString(i);
            // create ProducerRecord to send
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topic, value);

            // send data
            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    //executes everytime a record is successfully sent or thrown exception
                    if (exception == null) {
                        // record is successfully sent
                        logger.info("Received new metadata:\n" +
                                "Topic:" + metadata.topic() + "\n" +
                                "Partition:" + metadata.partition() + "\n" +
                                "Offset:" + metadata.offset() + "\n" +
                                "Timestamp:" + metadata.timestamp());
                    } else {
                        logger.error("Error while producing : " + exception.getLocalizedMessage());
                    }
                }
            });
        }
        //flush data
        kafkaProducer.flush();

        //close producer
        kafkaProducer.close();
    }
}
