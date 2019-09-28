

package com.flink;


import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import pojo.Student;
import pojo.User;

import java.util.ArrayList;
import java.util.List;

/**
 * 各种算子
 */
public class CalculateStreamingJob {


	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		testMap(env);
		env.execute();

	}


	/**
	 * 批处理wordCount
	 * @throws Exception
	 */
	public static void testWordCount_02() throws Exception{
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		String path = "/flink/input/a.log";
		env.readTextFile(path).flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
				for (String word : value.split(" ")) {
					collector.collect(new Tuple2(word, 1));
				}
			}
		}).groupBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
			@Override
			public Tuple2 reduce(Tuple2 t1, Tuple2 t2) throws Exception {
				Tuple2 t = new Tuple2();
				t.setField(t1.getField(0), 0);
				t.setField((Integer) t1.getField(1) + (Integer) t2.getField(1), 1);
				return t;
			}
		}).setParallelism(8).writeAsText("/flink/output");
		env.execute();
	}

	/**
	 *	流处理wordCount
	 * @param env
	 */
	public static void testWordCount_01(StreamExecutionEnvironment env){
		DataStreamSource<String> ds = env.readTextFile("F:\\a.java");
		ds.flatMap(new FlatMapFunction<String,WordCount>() {
			@Override
			public void flatMap(String value, Collector<WordCount> collector) throws Exception {
				for(String word:value.split(" ")){
					collector.collect(new WordCount(word,1));
				}
			}
		}).keyBy(new KeySelector<WordCount,String>() {
			@Override
			public String getKey(WordCount o) throws Exception {
				return o.getWord();
			}
		}).reduce(new ReduceFunction<WordCount>() {
			@Override
			public WordCount reduce(WordCount w1, WordCount w2) throws Exception {
				WordCount wc = new WordCount(w1.getWord(),w1.getCount()+w2.getCount());
				return wc;
			}
		}).setParallelism(1).writeAsText("F:\\a");
	}
	public static void testSplit(StreamExecutionEnvironment env){
		DataStreamSource ds = env.fromElements(1,2,3,4,5,6,7,8,9,10);
		ds.split(new OutputSelector<Integer>() {
			@Override
			public Iterable<String> select(Integer value) {
				List<String> out = new ArrayList<>();
				if(value %2==0){
					out.add("odd");
				}else{
					out.add("even");
				}
				return out;
			}
		}).select("odd","even").writeAsText("F:\\a");
	}

	public static void testConnect(StreamExecutionEnvironment env){
		DataStreamSource ds1 = env.fromElements(new Tuple2<String,Integer>("1",1));
		DataStreamSource ds2 = env.fromElements(1);
		ConnectedStreams ds = ds1.connect(ds2);
		ds.getFirstInput().writeAsText("F:\\a");
		ds.getSecondInput().writeAsText("F:\\a\\b");

	}
	public static void testUnion(StreamExecutionEnvironment env){
		DataStreamSource ds1 = env.fromElements(new Tuple2<String,Integer>("1",1));
		DataStreamSource ds2 = env.fromElements(new Tuple2<String,Integer>("b",2));
		DataStream ds = ds1.union(ds2);
		ds.writeAsText("F:\\a");
	}
	public static void testAggregation(StreamExecutionEnvironment env) throws Exception{

		env.fromElements(new Tuple2<String,Integer>("a",1),new Tuple2<String,Integer>("a",1))
				.keyBy(0).max(1).writeAsText("F:\\a");
	}

	public static void testKeyBy(StreamExecutionEnvironment env) throws Exception{
		List<String> list = new ArrayList<>();
		for(int i=0;i<100;i++){
			list.add("a"+i);list.add("a"+i);list.add("a"+i);
		}
		env.fromCollection(list).keyBy(new KeySelector<String, Object>() {
			@Override
			public Object getKey(String s) throws Exception {
				return s;
			}
		}).reduce(new ReduceFunction<String>() {
			@Override
			public String reduce(String s, String t1) throws Exception {
				return s+t1;
			}
		}).writeAsText("F:\\a");

	}

	public static void testFilter(StreamExecutionEnvironment env) throws Exception{
		List<Integer> list = new ArrayList<>();
		for(int i=0;i<100;i++){
			list.add(i);
		}
		SingleOutputStreamOperator out = env.fromCollection(list).filter(new FilterFunction<Integer>() {
			@Override
			public boolean filter(Integer integer) throws Exception {
				return integer%2==0;
			}
		});
		out.writeAsText("F:\\a");
	}

	public static void testFlatMap(StreamExecutionEnvironment env) throws Exception{

		SingleOutputStreamOperator out = env.readTextFile("F:\\input.log").flatMap(new FlatMapFunction<String, WordCount>() {
			@Override
			public void flatMap(String s, Collector<WordCount> collector) throws Exception {
				String[] line = s.toLowerCase().split(" ");
				if(line!=null && line.length>0){
					for(int i=0;i<line.length;i++){
						String word = line[i];
						WordCount wc = new WordCount(word,1);
						collector.collect(wc);
					}
				}
			}
		});
		out.writeAsText("F:\\a");
	}
	public static void testMap(StreamExecutionEnvironment env) throws Exception{
		List<Integer> list = new ArrayList<>();
		for(int i = 1;i<=10;i++){
			list.add(i);
		}
		SingleOutputStreamOperator<Integer> out = env.fromCollection(list).map(new MapFunction<Integer, Integer>() {
			@Override
			public Integer map(Integer i) throws Exception {
				return i*i;
			}
		});
		out.writeAsText("F:\\a");	//输出到磁盘
	}
}

class WordCount{
	private String word;
	private Integer count;

	public WordCount() {
	}

	public WordCount(String word, Integer count) {
		this.word = word;
		this.count = count;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return word+":"+count;
	}
}