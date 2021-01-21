import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Created by root on 2019/12/16.
 */
public class MyEventTimeExtractor implements TimestampExtractor{
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
        return System.currentTimeMillis();
    }
}
