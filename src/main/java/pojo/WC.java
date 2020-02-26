package pojo;

import java.util.ArrayList;
import java.util.List;

public class WC {
    private String word;
    private Integer cut;

    public WC() {
    }

    public WC(String word, Integer cut) {
        this.word = word;
        this.cut = cut;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getCut() {
        return cut;
    }

    public void setCut(Integer cut) {
        this.cut = cut;
    }

    @Override
    public String toString() {
        return "WC{" +
                "word='" + word + '\'' +
                ", cut=" + cut +
                '}';
    }

    public static List<WC> getWC(){
        List<WC> list = new ArrayList<>();
        for(int i=1;i<=100;i++){
            list.add(new WC("wc_"+i,1));
            list.add(new WC("wc_"+i,1));
            list.add(new WC("wc_"+i,1));
        }
        return list;
    }
}
