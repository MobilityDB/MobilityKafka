import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Named;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

public class WordCountExample {

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));

        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                "wordcount");
        /*props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");*/
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source =
                builder.stream("wordcount-input");
        final Pattern pattern = Pattern.compile("\\W+");
        KStream counts = source.flatMapValues(value->
                        Arrays.asList(pattern.split(value.toLowerCase())))
                .map((key, value) -> new KeyValue<Object,
                                        Object>(value, value))
                .filter((key, value) -> (!value.equals("the")))
                .groupByKey()
                .count(Named.as("CountStore")).mapValues(value->
                        Long.toString(value)).toStream();
        counts.to("wordcount-output");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        // This is for reset to work. Don't use in production - it causes the app to re-load the state from Kafka on every start
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        // Keep the stream running
        streams.start();
        // Block forever (or until Ctrl+C / container stop)
        Thread.currentThread().join();
    }
}
