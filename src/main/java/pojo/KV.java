package pojo;

public class KV {

    private Integer id;
    private Integer count;
    private Integer cnt;

    private String  userName;

    private String name;

    public KV() {
    }

    public KV(Integer id, Integer count) {
        this.id = id;
        this.count = count;
    }

    public Integer getCnt() {
        return cnt;
    }

    public void setCnt(Integer cnt) {
        this.cnt = cnt;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "KV{" +
                "id=" + id +
                ", count=" + count +
                '}';
    }
}
