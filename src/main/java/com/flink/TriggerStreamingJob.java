package com.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import utils.KafkaUtils;

import java.util.concurrent.ArrayBlockingQueue;

public class TriggerStreamingJob {
    public static void main(String[] args) throws Exception {


    }



    public static void testTimeWindow1() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds = env.addSource(new FlinkKafkaConsumer011<String>("test", new SimpleStringSchema(), KafkaUtils.getKfkPreperties()));
        ds.timeWindowAll(Time.milliseconds(1L)).trigger(CountTrigger.of(10));

        env.execute("execute");
    }
}

