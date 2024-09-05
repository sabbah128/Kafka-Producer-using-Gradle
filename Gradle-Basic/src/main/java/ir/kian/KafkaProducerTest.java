package ir.kian;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerTest {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerTest.class.getSimpleName());
    private static final String TOPIC = "data-stream";

    public static void main(String[] args) {
        log.info("Start Producing ...");

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9093, localhost:9094");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "1"); // acks only leader broker

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i=0; i<10; i++) {
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(TOPIC, "id_" + i, "Hello Kian_" + i);
            producer.send(producerRecord, (recordMetadata, e) -> {
                if (e == null) {
                    log.info("""
                                    Received new metadata...
                                    Key : {}
                                    Topic : {}
                                    Partitions : {}
                                    Offset : {}
                                    TimeStamp : {}
                                    """,
                            producerRecord.key(),
                            recordMetadata.topic(),
                            recordMetadata.partition(),
                            recordMetadata.offset(),
                            recordMetadata.timestamp());
                } else {
                    log.error("Error while producing : ", e);
                }
            });
        }

        producer.flush();
        producer.close();

    }
}
