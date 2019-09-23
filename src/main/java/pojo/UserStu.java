package pojo;

public class UserStu {
    private Integer id;
    private String userName;
    private Integer sid;
    private Integer level;

    public UserStu() {
    }

    public UserStu(Integer id, String userName, Integer sid, Integer level) {
        this.id = id;
        this.userName = userName;
        this.sid = sid;
        this.level = level;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
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
        return "UserStu{" +
                "id=" + id +
                ", userName='" + userName + '\'' +
                ", sid=" + sid +
                ", level=" + level +
                '}';
    }
}
