package utils;

import java.util.Properties;

public class KafkaUtils {
    public static Properties getKfkPreperties(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.234.129:9092");
        props.put("zookeeper.connect", "192.168.234.129:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");	//earliest,latest,和none
        return props;
    }
}
