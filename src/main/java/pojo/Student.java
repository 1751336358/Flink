package pojo;

import java.util.ArrayList;
import java.util.List;

public class Student {
    private Integer id;
    private Integer level;

    public Student(){

    }

    public Student(Integer id, Integer level) {
        this.id = id;
        this.level = level;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
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
                "id=" + id +
                ", level=" + level +
                '}';
    }

    public static List<Student> getStudent(){
        List<Student> list = new ArrayList<>();
        for(int i=0;i<100;i++){
            list.add(new Student(i+1,i*10));
        }
        return list;
    }
}
