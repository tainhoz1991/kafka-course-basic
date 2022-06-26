package demo.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import org.slf4j.LoggerFactory;

public class ProducerDemoCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoCallback.class);

    public static void main(String[] args) {
        log.info("I'm a Kafka Producer");
        // create Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());

        // create Producers
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i<10; i++){
            // create Producer Record
            ProducerRecord<String, String> record = new ProducerRecord<>("demo_java2", "hello tai");

            // send data asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // execute every time a record is successfully sent or an exception  is throw
                    if(e == null){
                        // the record was successfully sent
                        log.info("Received new metadata/ \n"+
                            "Topic: "+ recordMetadata.topic() +"\n"+
                            "Partition: "+ recordMetadata.partition() +"\n"+
                            "Offset: "+ recordMetadata.offset() +"\n"+
                            "Timestamp: "+ recordMetadata.timestamp()
                        );
                    } else {
                        log.error("Error while producing", e);
                    }
                }
            });
        }

        // flush
        producer.flush();

        producer.close();

    }

}
