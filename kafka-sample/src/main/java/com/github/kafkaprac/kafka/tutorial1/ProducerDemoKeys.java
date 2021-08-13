package com.github.kafkaprac.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithcallback.class);
        String bootstrapServers = "192.168.100.100:9092";
//        create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers );
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG ,StringSerializer.class.getName());

//      create the producer
        KafkaProducer<String, String> producer =
                new KafkaProducer<String, String>(properties);
    //  create a producer record
        for (int i = 0; i < 10; i++) {
            String topic = "first_topic";
            String value = "hello kafka" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);



//      send data - async
            producer.send(record, new Callback() {
                //          excutes every time a record is successfully sent or
//              or an exception is thrown
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
//              record was successfully sent
                    if (exception == null) {
                        logger.info("Receviced new metadata. \n " +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp()
                        );
                    } else {
                        logger.error("Error while producing", exception);
                    }
                }
            });
        }
//      flush data
        producer.flush();
//      close data
        producer.close();
    }
}
