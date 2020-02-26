package kafka._03sink_kfk;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * 自定义DataSink，写入kafka
 * 压力造数据命令
 * ./kafka-producer-perf-test.sh  --topic test --num-records 100000000 --record-size 687  --producer-props   bootstrap.servers=192.168.234.130:9092  batch.size=10000   --throughput 30000
 */
public class SinkToKfk {


	public static void main(String[] args) throws Exception {
		List<String> list = new ArrayList();
		for(int i=1;i<=1000;i++){
			list.add(UUID.randomUUID().toString());
		}
		//从List读取数据，写入kafka
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> ds = env.fromCollection(list);
		ds.addSink(new Sink2Kfk());
		env.execute("execute");
	}

}

/**
 * 写入到Kafka
 */
class Sink2Kfk extends RichSinkFunction<String> {
	PreparedStatement ps;

	private Producer<String, String> producer = null;
	private String topicName="sink_kfk";

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		producer = new KafkaProducer<String, String>(getProperties());
	}

	@Override
	public void close() throws Exception {
		super.close();
		if(producer != null){
			producer.close();
		}
	}

	@Override
	public void invoke(String str, Context context) throws Exception {
		Future<RecordMetadata> send = producer.send(new ProducerRecord<String, String>(topicName, str));   //写入String类型数据
		System.out.println("offset="+send.get().offset()+",partition="+send.get().partition()+",topic="+send.get().topic());

	}

	private static Properties getProperties(){
		Properties props = new Properties();
		props.put("bootstrap.servers","192.168.234.130:9092");
		props.put("acks","-1");
		props.put("retries",3);
		props.put("batch.size",16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}
}