package kafka._06wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import pojo.WC;
import utils.KafkaUtils;


public class KafkaStreamJob {


	public static void main(String[] args) throws Exception {
		wordcount();
	}


	//测试滚动窗口,每5s统计一次maxBy(score)
	public static void wordcount() throws Exception{
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getCheckpointConfig().setCheckpointInterval(10000L);
		//读取String类型并转化为Object，测试各种算子
		FlinkKafkaConsumer011<String> flinkKfkConsumer = new FlinkKafkaConsumer011<>("wc_topic", new SimpleStringSchema(), KafkaUtils.getKfkPreperties());
		flinkKfkConsumer.setStartFromEarliest();
		DataStreamSource<String> ds = env.addSource(flinkKfkConsumer);
		ds.flatMap(new FlatMapFunction<String, WC>() {
			@Override
			public void flatMap(String s, Collector<WC> collector) throws Exception {
				if(s != null && !"".equals(s)){
					String word[] = s.split(" ");
					for(String w:word){
						WC wc = new WC(w,1);
						collector.collect(wc);
					}
				}
			}
		}).setParallelism(2).keyBy("word").reduce(new ReduceFunction<WC>() {
			@Override
			public WC reduce(WC w1, WC w2) throws Exception {
				WC wc = new WC(w1.getWord(),w1.getCut()+w2.getCut());
				return wc;
			}
		}).setParallelism(2).writeAsText("/var/a");
		env.execute("execute");
	}


}