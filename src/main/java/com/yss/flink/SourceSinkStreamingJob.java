package com.yss.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 自定义DataSource 和DataSink
 */
public class SourceSinkStreamingJob {


	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        readToMySqlDataSource(env);
		env.execute("execute");

	}

    /**
     * 从mysql中读取，写入到mysql
     * @param env
     */
	public static void readToMySqlDataSource(StreamExecutionEnvironment env){
        DataStreamSource<Student> ds = env.addSource(new SourceToMySQL());
        ds.addSink(new SinkToMySQL());
    }

    /**
     * 从文件读取数据,写入到MySQL
     * 	1 zhangsan
     * 	2 lisi
     * @param env
     */
	public static void readToFileDataSource(StreamExecutionEnvironment env){
	    String path = "F:\\input";
        FileInputFormat inputFormat = new TextInputFormat(new Path(path));
	    DataStreamSource<String> ds = env.readFile(inputFormat,path,FileProcessingMode.PROCESS_CONTINUOUSLY,100);
	    ds.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String s) throws Exception {
                if(s!=null && !"".equals(s)){
                    String[] word = s.split(" ");
                    Integer id = Integer.parseInt(word[0]);
                    String name = word[1];
                    System.out.println(name);
                    return new Student(id,name);
                }
                return null;
            }
        }).addSink(new SinkToMySQL());
	}
}

/**
 * 从mysql读数据的数据源
 */
class SourceToMySQL extends RichSourceFunction<Student>{
    PreparedStatement ps;
    private Connection connection;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "select * from student;";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(connection != null)
            connection.close();
        if(ps != null)
            ps.close();
    }

    @Override
    public void run(SourceContext<Student> sourceContext) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            Student student = new Student(
            resultSet.getInt("id"),
            resultSet.getString("name"));
            sourceContext.collect(student);
        }
    }

    @Override
    public void cancel() {

    }

    private static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("org.gjt.mm.mysql.Driver");
           con = DriverManager.getConnection("jdbc:mysql://192.168.234.130:3306/xc_group?useUnicode=true&characterEncoding=UTF-8", "root", "root");
        } catch (Exception e) {
                System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage());
        }
        return con;
    }
}

/**
 * 写入到MySQL
 */
class SinkToMySQL extends RichSinkFunction<Student>{
	PreparedStatement ps;
    private Connection connection;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		connection = getConnection();
		String sql = "insert into student(id, name) values(?, ?);";
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
	public void invoke(Student student, Context context) throws Exception {
		ps.setInt(1,student.getId());
		ps.setString(2,student.getName());
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

/**
 * POJO
 */
class Student{

	private Integer id;
	private String name;

	public Student() {
	}

	public Student(Integer id, String name) {
		this.id = id;
		this.name = name;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

    @Override
    public String toString() {
        return "Student{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }
}