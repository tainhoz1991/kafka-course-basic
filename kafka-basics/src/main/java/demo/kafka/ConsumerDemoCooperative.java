package demo.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConsumerDemoCooperative {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class);

    public static void main(String[] args) {
        log.info("I'm a Kafka Consumer");
        String groupId = "my-group-consumer2";
        String topic = "demo_java2";
        // create Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
//            CooperativeStickyAssignor.class.getName());

        // create Consumers
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("Detected a Shutdown, let's exit by call method consumer.wakeup()...");
                consumer.wakeup();
                // join the main thread to allow the execution of the code in main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try{
            // subscribe consumer to our topic
            consumer.subscribe(Collections.singleton(topic));

            // poll for new data
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record: records){
                    log.info("Key: "+record.key()+". Value: "+record.value());
                    log.info("Partition: "+record.partition()+". Offset: "+record.offset());
                }
            }
        } catch (WakeupException e){
            log.info("Wake up exception");
        } catch (Exception e){
            log.info("Unexpected exception");
        } finally {
            consumer.close();
            log.info("The consumer is now gracefully closed");
        }


    }

}
