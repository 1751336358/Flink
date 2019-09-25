package kafka._02kafka_employ;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import pojo.Employ;

import java.util.Properties;

/**
 * 从kafka读取数据,测试各种算子
 */
public class KafkaSinkStreamingJob_02 {


	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		testTime(env);
		env.execute("execute");

	}

	public static void testTime(StreamExecutionEnvironment env){
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.234.130:9092");
		props.put("zookeeper.connect", "192.168.234.130:2181");
		props.put("group.id", "metric-group");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("auto.offset.reset", "latest");	//earliest,latest,和none

		//读取String类型并转化为Object，测试各种算子
		DataStreamSource<String> ds = env.addSource(new FlinkKafkaConsumer011<String>("employ_topic", new SimpleStringSchema(), props)).setParallelism(1);
		ds.map(new MapFunction<String, Employ>() {

			@Override
			public Employ map(String employStr) throws Exception {
				return JSON.parseObject(employStr,Employ.class);
			}
		}).timeWindowAll(Time.seconds(5L)).sum("score").print();
	}

}