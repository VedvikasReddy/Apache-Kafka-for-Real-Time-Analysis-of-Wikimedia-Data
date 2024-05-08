package io.conduktor.kafka.wikimedia;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class WikimediaEventHandler implements EventHandler {
    KafkaProducer<String, String> producer;
    String topic;
    private final Logger log = LoggerFactory.getLogger(WikimediaEventHandler.class.getSimpleName());
    public WikimediaEventHandler(KafkaProducer<String, String> kafkaProducer, String kafkaTopic){
        this.producer = kafkaProducer;
        this.topic = kafkaTopic;
    }
    @Override
    public void onOpen() {

    }
    @Override
    public void onClosed() {
        producer.close();
    }
    @Override
    public void onMessage(String event, MessageEvent mEvent) {
        producer.send(new ProducerRecord<>(topic, mEvent.getData()));
        log.info(mEvent.getData());
    }
    @Override
    public void onComment(String comment) {

    }
    @Override
    public void onError(Throwable error) {
        log.error("There is an error in stream reading!!!!!", error);
    }
}
