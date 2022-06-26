package demo;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangesHandle implements EventHandler {
    KafkaProducer<String, String> kafkaProducer;
    String topic;
    private final Logger logger = LoggerFactory.getLogger(WikimediaChangesHandle.class.getSimpleName());

    public WikimediaChangesHandle(KafkaProducer<String, String> producer, String topic){
        this.kafkaProducer = producer;
        this.topic = topic;
    }

    // open connect to stream
    @Override
    public void onOpen() {
        // we don't need to do nothing here
    }

    @Override
    public void onClosed() {
        // we are closing reading from stream, we close kafka producer
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        // The stream has received a message coming from HTTP stream
        // we want to send it through the kafka producer
        // we use asynchronous code (bat dong bo)

        logger.info(messageEvent.getData()); // getData() return content of message
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String s) {
        // nothing here
    }

    @Override
    public void onError(Throwable t) {
        logger.error("Error in stream reading",t);
    }
}
