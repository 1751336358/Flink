package com.flink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import pojo.Student;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class TimeAndWatermarksStreamingJob {
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
        List<Student> student = Student.getStudent();
        env.addSource(new SourceFunction<Student>() {
            @Override
            public void run(SourceContext<Student> ctx) throws Exception {
                for(Student stu:student){
                    ctx.collectWithTimestamp(stu,System.currentTimeMillis());
                    ctx.emitWatermark(new Watermark(1L));
                }
            }

            @Override
            public void cancel() {

            }
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Student>() {
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return null;
            }

            @Override
            public long extractTimestamp(Student student, long l) {
                return 0;
            }
        });
    }

    /**
     * 测试滑动窗口
     * @param env
     */
    public static void testTimeWindow2(StreamExecutionEnvironment env){
        ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue(10000);
        for(int i = 0;i<10000;i++){
            queue.add(i);
        }
        //每1ms求前10ms内的最大值
        DataStreamSource<Integer> ds = env.fromCollection(queue);
        ds.timeWindowAll(Time.milliseconds(10L),Time.milliseconds(1)).sum(0).writeAsText("F:\\a");
    }
    /**
     * 测试滚动窗口
     * @param env
     */
    public static void testTimeWindow1(StreamExecutionEnvironment env){
        ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue(10000);
        for(int i = 0;i<10000;i++){
            queue.add(i);
        }
        DataStreamSource<Integer> ds = env.fromCollection(queue);
        ds.timeWindowAll(Time.milliseconds(1L)).max(0).writeAsText("F:\\a");
    }
    /**
     * 测试countWindow
     * @param env
     */
    public static void testCountWindow(StreamExecutionEnvironment env){
        ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue(10000);
        for(int i = 0;i<1000;i++){
            queue.add(i);
        }
        DataStreamSource<Integer> ds = env.fromCollection(queue);
        ds.countWindowAll(10).sum(0).writeAsText("F:\\a");
    }
}
