package com.flink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import pojo.*;
import pojo.Student;

import java.util.ArrayList;
import java.util.List;

public class TableSqlJob {

    public static void main(String[] args) throws Exception {
        filter();
    }

    //测试filter
    public static void filter() throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tabEnv = TableEnvironment.getTableEnvironment(env);
        DataSource<Student> ds = env.fromCollection(Student.getStudent());
        tabEnv.registerDataSet("stu",ds);
        Table table = tabEnv.scan("stu").filter("sid%2=0").filter("sid!=100").select("sid,level");
        tabEnv.toDataSet(table,Student.class).print();
    }
    public static void testUnionAll() throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tabEnv = TableEnvironment.getTableEnvironment(env);
        DataSource<WC> ds = env.fromCollection(WC.getWC());
        tabEnv.registerDataSet("wc1",ds);
        tabEnv.registerDataSet("wc2",ds);
        Table wc1 = tabEnv.scan("wc1");
        Table wc2 = tabEnv.scan("wc2");
        Table tab = wc1.unionAll(wc2).select("*");
        tabEnv.toDataSet(tab,WC.class).print();
    }

    //rightJoin
    public static void testRightJoin() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        tabEnv.registerDataStream("user",env.fromCollection(getUserList()));
        tabEnv.registerDataStream("stu",env.fromCollection(Student.getStudent()));
        //select * from user right join stu on id=sid where id<35  以右表为基准
        Table table = tabEnv.sqlQuery("select * from `user` right join  `stu` on id=sid where id<35");
        tabEnv.toRetractStream(table,UserStu.class).print();
        env.execute();

    }

    public static void testTableSQL_01() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        tabEnv.registerDataStream("user",env.fromCollection(getUserList()),"id,userName");
        //表必须用` `包住
        Table tab = tabEnv.sqlQuery("SELECT id,userName FROM `user` where id<50");
        tabEnv.toAppendStream(tab,User.class).print();
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
        for (int i = 1; i <= 30; i++) {
            list.add(new User(1, "name" + i));
            list.add(new User(1, "name" + i));
            list.add(new User(1, "name" + i));
        }
        return list;
    }
}
