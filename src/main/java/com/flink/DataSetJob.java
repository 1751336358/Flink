package com.flink;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DataSetJob {
    public static void main(String[] args) throws Exception {
    }



    //先用_1分组，在用_0排序，如果有多个_0相同，则根据_2排序
    public static void minBy() throws Exception{
        List<Tuple3<Integer,String,Integer>> list = new ArrayList<>();
        list.add(new Tuple3<>(1,"Hello",5));
        list.add(new Tuple3<>(1,"Hello",4));
        list.add(new Tuple3<>(2,"Hello",5));
        list.add(new Tuple3<>(3,"World",7));
        list.add(new Tuple3<>(4,"World",6));
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.fromCollection(list).groupBy(1).minBy(2,0).print();
    }

    public static void sort() throws Exception{
        List<Tuple2<Integer,Integer>> list1 = new ArrayList<>();
        list1.add(new Tuple2<>(3,3));
        list1.add(new Tuple2<>(4,4));
        list1.add(new Tuple2<>(1,1));
        list1.add(new Tuple2<>(2,2));
        list1.add(new Tuple2<>(2,3));
        list1.add(new Tuple2<>(2,1));
        ExecutionEnvironment env = getExecutionEnvironment();

        env.fromCollection(list1).sortPartition(0, Order.DESCENDING).sortPartition(1,Order.ASCENDING).print();
    }

    public static void union() throws Exception{
        List<Tuple2<String,String>> list1 = new ArrayList<>();
        list1.add(new Tuple2<>("aaa","111"));
        list1.add(new Tuple2<>("bbb","222"));
        list1.add(new Tuple2<>("ccc","333"));
        list1.add(new Tuple2<>("ddd","444"));
        List<Tuple2<String,String>> list2 = new ArrayList<>();
        list2.add(new Tuple2<>("eee","555"));
        list2.add(new Tuple2<>("fff","666"));
        list2.add(new Tuple2<>("ggg","777"));
        ExecutionEnvironment env = getExecutionEnvironment();

        env.fromCollection(list1).union(env.fromCollection(list2)).print();
    }
    public static void cross() throws Exception{
        List<Tuple2<String,String>> list1 = new ArrayList<>();
        list1.add(new Tuple2<>("aaa","111"));
        list1.add(new Tuple2<>("bbb","222"));
        list1.add(new Tuple2<>("ccc","333"));
        list1.add(new Tuple2<>("ddd","444"));
        List<Tuple2<String,String>> list2 = new ArrayList<>();
        list2.add(new Tuple2<>("eee","555"));
        list2.add(new Tuple2<>("fff","666"));
        list2.add(new Tuple2<>("ggg","777"));

        ExecutionEnvironment env = getExecutionEnvironment();
        //cross，笛卡尔积
        env.fromCollection(list1).cross(env.fromCollection(list2)).with(new CrossFunction<Tuple2<String,String>, Tuple2<String,String>, Object>() {
            @Override
            public Tuple2<String, String> cross(Tuple2<String, String> left, Tuple2<String, String> right) throws Exception {
                return new Tuple2<String, String>(left.getField(0)+"|"+right.getField(0),left.getField(1)+"|"+right.getField(1));
            }
        }).print();
    }
    //leftOuterJoin与rightOuterJoin类似,不做演示
    public static void leftOuterJoin() throws Exception{
        List<Tuple2<String,String>> list1 = new ArrayList<>();
        list1.add(new Tuple2<>("aaa","111"));
        list1.add(new Tuple2<>("bbb","222"));
        list1.add(new Tuple2<>("ccc","333"));
        list1.add(new Tuple2<>("ddd","444"));
        List<Tuple2<String,String>> list2 = new ArrayList<>();
        list2.add(new Tuple2<>("ccc","xxx"));
        list2.add(new Tuple2<>("fff","666"));
        list2.add(new Tuple2<>("ggg","777"));

        ExecutionEnvironment env = getExecutionEnvironment();
        //leftOuterJoin<==>left join
        env.fromCollection(list1).leftOuterJoin(env.fromCollection(list2)).where(0).equalTo(0).with(new JoinFunction<Tuple2<String,String>, Tuple2<String,String>, Object>() {
            @Override
            public Tuple2<String, String> join(Tuple2<String, String> left, Tuple2<String, String> right) throws Exception {
                if(right == null){
                    return left;
                }
                return right;
            }
        }).print();
    }
    public static void join() throws Exception{
        List<Tuple2<String,String>> list1 = new ArrayList<>();
        list1.add(new Tuple2<>("aaa","111"));
        list1.add(new Tuple2<>("bbb","222"));
        list1.add(new Tuple2<>("ccc","333"));
        List<Tuple2<String,String>> list2 = new ArrayList<>();
        list2.add(new Tuple2<>("ddd","444"));
        list2.add(new Tuple2<>("eee","555"));
        list2.add(new Tuple2<>("aaa","666"));

        ExecutionEnvironment env1 = getExecutionEnvironment();
        DataSource ds1 = env1.fromCollection(list1);
        DataSource ds2 = env1.fromCollection(list2);
        ds1.join(ds2).where(0).equalTo(0).print();
    }
    public static void distinct() throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(111,222,333,444,555,111,222).distinct().print();
    }

    private static ExecutionEnvironment getExecutionEnvironment(){
        return ExecutionEnvironment.getExecutionEnvironment();
    }
}
