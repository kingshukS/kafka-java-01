package kafka.tutorial01.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoCallbackWithKeys {

    private static final String BOOTSTRAP_SERVER = "localhost:9092";


    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoCallbackWithKeys.class);
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);

        for(int i = 0; i < 10; i++) {

            String topic = "first_topic";
            String key = "id_"+Integer.toString(i);
            String value = "Hello Kingshuk : " + Integer.toString(i);
            // create ProducerRecord to send
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topic, key, value);

            //log the key
            logger.info("Key : "+key);

            // Same key will go to same partition no matter how many times you run it
           /* mapping below:
            id_0 -> Partition:1
            id_1 -> Partition:0
            id_2 -> Partition:2
            id_8 -> Partition:1
            id_9 -> Partition:2*/

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
            }).get(); // block it to make it synchronous which is not recommended in production
        }
        //flush data
        kafkaProducer.flush();

        //close producer
        kafkaProducer.close();
    }
}
