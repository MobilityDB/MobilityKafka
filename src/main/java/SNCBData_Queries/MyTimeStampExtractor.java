package SNCBData_Queries;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.format.DateTimeFormatter;

public class MyTimeStampExtractor implements TimestampExtractor {

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        String value = (String) record.value();
        if (value != null && !value.trim().isEmpty()) {
            try {
                String[] cols = value.split(",");
                return Long.parseLong(cols[0].trim())*1000L;
            } catch (Exception e) {
                // header row or malformed line
            }
        }
        return partitionTime;
    }
}