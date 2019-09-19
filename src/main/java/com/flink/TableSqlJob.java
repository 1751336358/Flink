package com.flink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import pojo.Student;
import pojo.User;

import java.util.ArrayList;
import java.util.List;

public class TableSqlJob {

    public static void main(String[] args) throws Exception {
        testJoin();
    }

    public static void testJoin() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
        Table user = tabEnv.fromDataStream(env.fromCollection(getUserList()), "id,userName");
        Table stu = tabEnv.fromDataStream(env.fromCollection(Student.getStudent()),"sid,level");
        Table leftOuterJoin = user.leftOuterJoin(stu).where("id=sid").select("userName");
        tabEnv.toAppendStream(leftOuterJoin,String.class).print();
        env.execute();
    }

    //测试简单的join操作
    public static void join() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table user = tableEnv.fromDataStream(env.fromCollection(getUserList()),"id,userName");
        Table stu = tableEnv.fromDataStream(env.fromCollection(Student.getStudent()),"sid,level");
        //第一和where，指定关联条件，第二个where，数据过滤
        //select * from user join stu on id=sid where sid%2=0 and level=760
        Table join = user.join(stu).where("id=sid").select("sid,level").where("sid%2=1&&level=760");

        tableEnv.toAppendStream(join,Student.class).print();
        env.execute();
    }

    //测试Stream转Table
    public static void testStreamToTable() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.registerDataStream("user", env.fromCollection(getUserList()));    //注册一张User表
        Table table = tableEnv.scan("user").select("id").where("id%2===0");    //select id 与Integer.class映射
        tableEnv.toAppendStream(table, Integer.class).print();
        env.execute();
    }

    //测试DataStream转Table
    public static void testFromDataStreamFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table table = tableEnv.fromDataStream(env.fromCollection(getUserList())).select("*").where("id % 2 === 1");        //select * 与User.class映射
        tableEnv.toAppendStream(table, User.class).print();
        env.execute();
    }

    //批处理
    public static void testBatchTable() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
        DataSource<User> ds = env.fromCollection(getUserList());
        tableEnv.registerDataSet("user", ds);

        Table table = tableEnv.scan("user").select("*").where("id % 2 === 0");
        table.printSchema();
    }

    //流处理
    public static void testStreamTable() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        DataStreamSource<User> ds = env.fromCollection(getUserList());
        tableEnv.registerDataStream("user", ds);

        Table table = tableEnv.scan("user").select("*").where("id % 2 === 0");
        table.printSchema();
    }

    public static List<User> getUserList() {
        List<User> list = new ArrayList();
        for (int i = 0; i < 100; i++) {
            list.add(new User(i, "name" + i));
        }
        return list;
    }
}
