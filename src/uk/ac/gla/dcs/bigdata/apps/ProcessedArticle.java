package uk.ac.gla.dcs.bigdata.apps;
import java.io.Serializable;
import java.util.List;
import java.util.HashMap;

public class ProcessedArticle implements Serializable {

    private static final long serialVersionUID = 7860293794072512243L;

    String id;
    HashMap<String, Integer> tokenCounts;
    int totalTokenCount;

    public ProcessedArticle() {}
    public ProcessedArticle(String id, HashMap<String, Integer> tokenCounts, int totalTokenCount) {
        this.id = id;
        this.tokenCounts = tokenCounts;
        this.totalTokenCount = totalTokenCount;
    }

    //do all getters and setters please
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public HashMap<String, Integer> getTokenCounts() {
        return tokenCounts;
    }

    public void setTokenCounts(HashMap<String, Integer> tokenCounts) {
        this.tokenCounts = tokenCounts;
    }

    public int getTotalTokenCount() {
        return totalTokenCount;
    }

    public void setTotalTokenCount(int totalTokenCount) {
        this.totalTokenCount = totalTokenCount;
    }

    public String toString() {
        return "ID: " + id + " Tokens: " + tokenCounts.toString();
    }
    
}
