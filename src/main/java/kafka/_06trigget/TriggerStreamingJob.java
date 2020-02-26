package kafka._06trigget;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import pojo.Employ;
import utils.KafkaUtils;

public class TriggerStreamingJob {
    public static void main(String[] args) throws Exception {
        testCountTrigger();

    }


    /**
     * 测试flink自带的CountTrigger
     * 如果在窗口时间范围内，符合条件的数据个数超过Trigger指定的个数，就触发window计算
     * @throws Exception
     */
    public static void testCountTrigger() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.addSource(new FlinkKafkaConsumer011<String>("trigger_topic", new SimpleStringSchema(), KafkaUtils.getKfkPreperties()));
        ds.map(new MapFunction<String, Employ>() {
            @Override
            public Employ map(String s) throws Exception {
                return JSON.parseObject(s,Employ.class);
            }
        }).keyBy("name").timeWindow(Time.seconds(5)).trigger(CountTrigger.of(10)).maxBy("score").print();
        env.execute("execute");
    }
}

