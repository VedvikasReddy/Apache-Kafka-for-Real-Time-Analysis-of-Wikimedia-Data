package io.conduktor.kafka.opensearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "https://f6gbeuapnj:ra7c7uq6ss@kafka-adv-project-3714983080.us-west-2.bonsaisearch.net:443";
        URI connectionURI = URI.create(connString);
        RestHighLevelClient highLevelClient;
        String userInfo = connectionURI.getUserInfo();

        if (userInfo == null) {
            highLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connectionURI.getHost(), connectionURI.getPort(),"http")));

        } else {
            String[] auth = userInfo.split(":");

            CredentialsProvider credProvider = new BasicCredentialsProvider();
            credProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            highLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connectionURI.getHost(), connectionURI.getPort(), connectionURI.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credProvider)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return highLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(){

        String grpId = "consumer-opensearch";
        Properties props = new Properties();
        props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, grpId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return new KafkaConsumer<>(props);

    }


    public static void main(String[] args) throws IOException {

        Logger logger = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());
        RestHighLevelClient client = createOpenSearchClient();
        KafkaConsumer<String, String> openSearchconsumer = createKafkaConsumer();
        try(client; openSearchconsumer){
            boolean indexPresent = client.indices().exists(new GetIndexRequest("project"), RequestOptions.DEFAULT);
            if (!indexPresent){
                CreateIndexRequest createIndex = new CreateIndexRequest("project");
                client.indices().create(createIndex, RequestOptions.DEFAULT);
                logger.info("A new index named project has been created!");
            } else {
                logger.info("The index already exits");
            }
            openSearchconsumer.subscribe(Collections.singleton("advdb-project"));
            while(true) {
                ConsumerRecords<String, String> cR = openSearchconsumer.poll(Duration.ofMillis(3000));
                int count = cR.count();
                logger.info("OpenSearch has received " + count + " record(s)");
                for (ConsumerRecord<String, String> record : cR) {
                    try {

                        IndexRequest iR = new IndexRequest("project")
                                .source(record.value(), XContentType.JSON);
                        IndexResponse indResponse = client.index(iR, RequestOptions.DEFAULT);
                        logger.info(indResponse.getId());
                    } catch (Exception e){

                    }

                }
            }
        }
    }


}