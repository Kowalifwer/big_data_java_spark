package uk.ac.gla.dcs.bigdata.apps;
import java.io.Serializable;

public class QueryResult implements Serializable {
    private static final long serialVersionUID = 7860244794072512243L;

    private ProcessedArticle processedArticle;
    private double score;

    public QueryResult() {}
    public QueryResult(ProcessedArticle processedArticle, double score) {
        this.processedArticle = processedArticle;
        this.score = score;
    }

    public ProcessedArticle getProcessedArticle() {
        return processedArticle;
    }

    public void setProcessedArticle(ProcessedArticle processedArticle) {
        this.processedArticle = processedArticle;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public String toString() {
        return "Score: " + score;
    }
}
