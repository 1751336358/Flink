package kafka._01_kafka_demo;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * 自定义DataSource 和DataSink
 * 从kafka读取数据，写入mysql
 */
public class KafkaSinkStreamingJob_01 {


	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		readFromKafka(env);
		env.execute("execute");

	}

	public static void readFromKafka(StreamExecutionEnvironment env){
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.234.130:9092");
		props.put("zookeeper.connect", "192.168.234.130:2181");
		props.put("group.id", "metric-group");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("auto.offset.reset", "latest");	//earliest,latest,和none

		//flink自带的从kafka读入工具,(topic,字符解码器,属性文件)
		DataStreamSource<String> ds = env.addSource(new FlinkKafkaConsumer011<String>("test", new SimpleStringSchema(), props)).setParallelism(1);
		ds.addSink(new Sink2MySQL_01());
	}

}

/**
 * 写入到MySQL
 */
class Sink2MySQL_01 extends RichSinkFunction<String> {
	PreparedStatement ps;
	private Connection connection;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		connection = getConnection();
		String sql = "insert into kafka_tab(str_str) values(?);";	//表结构只有一个str_str varchar(1024)字段
		ps = this.connection.prepareStatement(sql);
	}

	@Override
	public void close() throws Exception {
		super.close();
		if(connection != null){
			connection.close();
		}
		if(ps != null){
			ps.close();
		}
	}

	@Override
	public void invoke(String str, Context context) throws Exception {
		ps.setString(1,str);
		ps.executeUpdate();
	}

	private static Connection getConnection() {
		Connection con = null;
		try {
			Class.forName("org.gjt.mm.mysql.Driver");
			con = DriverManager.getConnection("jdbc:mysql://192.168.234.130:3306/xc_group?useUnicode=true&characterEncoding=UTF-8", "root", "root");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage());
		}
		return con;
	}
}