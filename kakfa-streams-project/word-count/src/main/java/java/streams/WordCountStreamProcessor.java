package java.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class WordCountStreamProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountStreamProcessor.class);

    public static void main(String[] args) {
        new WordCountStreamProcessor().run();
    }

    private void run() {
        Properties kafkaProperties = createKafkaProperties();

        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> wordCountStream = builder.stream("word-count-topic", Consumed.with(stringSerde, stringSerde));

        KTable<String, Long> wordCountKeyValueStream = wordCountStream.mapValues(new ValueMapper<String, String>() {
            @Override
            public String apply(String value) {
                return value.toLowerCase();
            }
        }).flatMapValues(new ValueMapper<String, Iterable<String>>() {
            @Override
            public Iterable<String> apply(String value) {
                return Arrays.asList(value.split(" "));
            }
        }).selectKey((String ignoredKey, String key) -> key).groupByKey().count();

        wordCountKeyValueStream.toStream().to("word-count-output", Produced.with(stringSerde, Serdes.Long()));


        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), kafkaProperties);

        kafkaStreams.start();
        LOGGER.info(kafkaStreams.toString());

        //Shutdown hook to correctly close the stream application.
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    private Properties createKafkaProperties() {
        Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "rsvp-stream-processor");

        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return properties;
    }
}
