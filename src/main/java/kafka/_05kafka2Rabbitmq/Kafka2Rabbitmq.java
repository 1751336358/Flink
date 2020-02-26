package kafka._05kafka2Rabbitmq;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import utils.KafkaUtils;

/**
 * 从kafka读取数据，写入rabbitmq
 * 压力造数据命令
 * ./kafka-producer-perf-test.sh  --topic test --num-records 100000000 --record-size 687  --producer-props   bootstrap.servers=192.168.234.130:9092  batch.size=10000   --throughput 30000
 */
public class Kafka2Rabbitmq {


	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		rmq(env);
		env.execute("execute");

	}

    /**
     * 从kafka读取数据写入rabbitmq
	 * @param env
	 */
	public static void rmq(StreamExecutionEnvironment env){
		DataStreamSource<String> ds = env.addSource(new FlinkKafkaConsumer011<String>("test", new SimpleStringSchema(), KafkaUtils.getKfkPreperties())).setParallelism(1);
			RMQConnectionConfig config = new RMQConnectionConfig.Builder()
					.setHost("s-rabbitmq1.qa.bj4.daling.com")
					.setVirtualHost("/" +
							"xcpcenter")
					.setPort(5672)
					.setUserName("daling")
					.setPassword("daling")
					.build();

		ds.addSink(new RMQSink<String>(config,"push_return",new SimpleStringSchema()));

	}

}
