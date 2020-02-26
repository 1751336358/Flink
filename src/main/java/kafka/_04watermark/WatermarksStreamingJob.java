package kafka._04watermark;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import pojo.MyEvent;
import utils.KafkaUtils;

import javax.annotation.Nullable;

public class WatermarksStreamingJob {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        watermarks(env);
        env.execute("execute");
    }

    /**
     * 设置时间戳和水位
     * @param env
     * @throws Exception
     */
    public static void watermarks(StreamExecutionEnvironment env)throws Exception{
        FlinkKafkaConsumer011<String> flinkKfkConsumer = new FlinkKafkaConsumer011<>("myEvent_topic", new SimpleStringSchema(), KafkaUtils.getKfkPreperties());
        env.addSource(flinkKfkConsumer).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {

            long maxOutOfOrderness = 1010L;
            long currentMaxTimestamp;
            @Override
            public long extractTimestamp(String event, long l) {
                MyEvent myEvent = JSON.parseObject(event,MyEvent.class);
                long timeStamp = myEvent.getDelayTime();
                currentMaxTimestamp = Math.max(currentMaxTimestamp,timeStamp);
                return timeStamp;
            }

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                //计算水印
                return new Watermark(currentMaxTimestamp-maxOutOfOrderness);
            }
        }).timeWindowAll(Time.seconds(1)).max("0").print();
    }

    
}
