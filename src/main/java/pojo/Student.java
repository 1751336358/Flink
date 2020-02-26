package pojo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Student implements Serializable {

    private Integer sid;
    private Integer level;

    public Student(){

    }

    public Student(Integer sid, Integer level) {
        this.sid = sid;
        this.level = level;
    }

    public Integer getSid() {
        return sid;
    }

    public void setSid(Integer sid) {
        this.sid = sid;
    }

    public Integer getLevel() {
        return level;
    }

    public void setLevel(Integer level) {
        this.level = level;
    }

    @Override
    public String toString() {
        return "Student{" +
                "sid=" + sid +
                ", level=" + level +
                '}';
    }

    public static List<Student> getStudent(){
        List<Student> list = new ArrayList<>();
        for(int i=1;i<=100;i++){
            list.add(new Student(i,i*10));
        }
        return list;
    }
}
