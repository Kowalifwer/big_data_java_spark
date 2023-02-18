package uk.ac.gla.dcs.bigdata.apps;
import java.io.Serializable;
import java.util.Map;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class ProcessedArticle implements Serializable {

    private static final long serialVersionUID = 7860293794072512243L;

    NewsArticle newsArticle;
    Map<String, Integer> tokenCounts;
    int totalTokenCount;

    public ProcessedArticle() {}
    public ProcessedArticle(NewsArticle newsArticle, Map<String, Integer> tokenCounts, int totalTokenCount) {
        this.newsArticle = newsArticle;
        this.tokenCounts = tokenCounts; //this is token:count map
        this.totalTokenCount = totalTokenCount;
    }

    public NewsArticle getNewsArticle() {
        return newsArticle;
    }

    public void setNewsArticle(NewsArticle newsArticle) {
        this.newsArticle = newsArticle;
    }

    public Map<String, Integer> getTokenCounts() {
        return tokenCounts;
    }

    public void setTokenCounts(Map<String, Integer> tokenCounts) {
        this.tokenCounts = tokenCounts;
    }

    public int getTotalTokenCount() {
        return totalTokenCount;
    }

    public void setTotalTokenCount(int totalTokenCount) {
        this.totalTokenCount = totalTokenCount;
    }

    public String toString() {
        return "Tokens: " + tokenCounts.toString();
    }
    
}
