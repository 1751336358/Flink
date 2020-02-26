package pojo;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MyEvent {
    private Integer id;
    private Long creationTime = System.currentTimeMillis(); //事件生成的时间
    private Long delayTime;         //延迟时间

    public MyEvent() {
    }

    public MyEvent(Integer id) {
        this.id = id;
    }

    public MyEvent(Integer id, Long creationTime) {
        this.id = id;
        this.creationTime = creationTime;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(Long creationTime) {
        this.creationTime = creationTime;
    }

    public Long getDelayTime() {
        return delayTime;
    }

    public void setDelayTime(Long delayTime) {
        this.delayTime = delayTime;
    }

    public static List<MyEvent> getMyEventList(){
        List<MyEvent> list =  new ArrayList<>();
        for(int i=0;i<=100;i++){
            list.add(new MyEvent(i));
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
            }
        }
        return list;
    }

    @Override
    public String toString() {
        return "MyEvent{" +
                "id=" + id +
                ", creationTime=" + new SimpleDateFormat("yyyy-MM-dd:hh:mm:ss:SSS").format(new Date(creationTime)) +
                ", delayTime=" + new SimpleDateFormat("yyyy-MM-dd:hh:mm:ss:SSS").format(new Date(delayTime)) +
                '}';
    }
}
