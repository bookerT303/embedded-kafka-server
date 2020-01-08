package io.pivotal.kafka.embeddedkafkabroker;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static java.util.stream.Collectors.toList;

@SpringBootApplication
public class EmbeddedKafkaServer {

    private static Logger log = LoggerFactory.getLogger(EmbeddedKafkaServer.class);

    private static String[] topics = {"no-topics-specified"};

    public static void main(String[] args) {
        if (args.length == 0) {
            log.info("Usage: EmbeddedKafkaServer topic(s)");
            System.exit(-1);
        }
        topics = args;
        SpringApplication.run(EmbeddedKafkaServer.class, args);
    }

    private EmbeddedKafkaBroker broker;

    public EmbeddedKafkaServer(@Qualifier("EmbeddedKafkaBroker") EmbeddedKafkaBroker broker) {
        this.broker = broker;
    }

    @Bean("EmbeddedKafkaBroker")
    public static EmbeddedKafkaBroker broker() throws Exception {
        log.info("broker starting with topics {}", Arrays.stream(topics).collect(toList()));
        EmbeddedKafkaBroker embeddedKafkaBroker = new EmbeddedKafkaBroker(1, false,
                topics);
        embeddedKafkaBroker.brokerProperties(convert(System.getProperties()));
        embeddedKafkaBroker.kafkaPorts(9092);
        return embeddedKafkaBroker;
    }

    private static Map<String, String> convert(Properties properties) throws Exception {
        Map<String, String> map = new HashMap<>();
        for (Object key : properties.keySet()) {
            String skey = key.toString();
            map.put(skey, properties.getProperty(skey));
        }
        return map;
    }
}
