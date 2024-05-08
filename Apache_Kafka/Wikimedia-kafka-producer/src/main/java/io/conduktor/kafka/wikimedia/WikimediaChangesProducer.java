package io.conduktor.kafka.wikimedia;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {
        String dataUrl = "https://stream.wikimedia.org/v2/stream/recentchange";
        String topic = "advdb-project";
        Properties kProps = new Properties();
        kProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        kProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> mP = new KafkaProducer<>(kProps);
        EventHandler handler = new WikimediaEventHandler(mP, topic);
        EventSource.Builder eventSourceBuilder = new EventSource.Builder(handler, URI.create(dataUrl));
        EventSource streamingEventSource = eventSourceBuilder.build();
        streamingEventSource.start();
        TimeUnit.MINUTES.sleep(10);

    }
}
