import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 * Created by root on 2019/12/16.
 *
 * 输入topic：log
 * MOVIE_RATING_PREFIX:1|2|5.0|1564412038
 *
 * 输出topic：recom
 * 1|2|5.0|1564412038
 */
public class Application {

    public static void main(String[] args) {

        String brokers = "172.16.121.3:9092";
        String zookeeper = "172.16.121.3:2181";

        String fromTopic = "calllog";
        String toTopic = "calllog133";

        Properties settings = new Properties();

        settings.put(StreamsConfig.APPLICATION_ID_CONFIG,"logFilter");//相当于 setname
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,brokers);
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG,zookeeper);

        settings.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG,MyEventTimeExtractor.class.getName());

        StreamsConfig config = new StreamsConfig(settings);

        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("SOURCE",fromTopic)
                .addProcessor("PROCESS",()->new LogProcessor(),"SOURCE")
                .addSink("SINK",toTopic,"PROCESS");

        KafkaStreams streams = new KafkaStreams(builder,config);
        streams.start();
    }
}
