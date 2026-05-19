import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

import java.util.Properties;

public class AISDataCountExample {

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("wordcount-input");

        source
                .filter((key, value) -> !value.startsWith("t,"))        // skip header row
                .map((key, value) -> {
                    String[] cols = value.split(",");
                    String mmsi = cols[1].trim();                        // extract mmsi column
                    return new KeyValue<>(mmsi, mmsi);
                })
                .groupByKey()
                .count(Named.as("CountStore"))
                .mapValues(v -> Long.toString(v))
                .toStream()
                .to("wordcount-output");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        Thread.currentThread().join();
    }
}

