package pojo;

public class Group {
    private String userName;
    private Long ids;

    public Group() {
    }

    public Group(String userName, Long ids) {
        this.userName = userName;
        this.ids = ids;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Long getIds() {
        return ids;
    }

    public void setIds(Long ids) {
        this.ids = ids;
    }
}
