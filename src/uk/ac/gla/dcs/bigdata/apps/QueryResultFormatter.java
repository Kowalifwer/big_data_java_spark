package uk.ac.gla.dcs.bigdata.apps;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import java.util.Map;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;


public class QueryResultFormatter implements MapFunction<ProcessedArticle, QueryResult> {
    double averageTokenCountPerDocument;
    Broadcast<Map<String, Integer>> globalTokenCountMap;
    Broadcast<Map<String, Short>> queryTokenCounts;
    long totalDocumentsInCorpus;

    public QueryResultFormatter(double averageTokenCountPerDocument, Broadcast<Map<String, Integer>> globalTokenCountMap, Broadcast<Map<String, Short>> queryTokenCounts, long totalDocumentsInCorpus) {
        this.averageTokenCountPerDocument = averageTokenCountPerDocument;
        this.globalTokenCountMap = globalTokenCountMap;
        this.queryTokenCounts = queryTokenCounts;
        this.totalDocumentsInCorpus = totalDocumentsInCorpus;
    }

    @Override
    public QueryResult call(ProcessedArticle processedArticle) throws Exception {
        double score = 0;
        for(String queryToken : queryTokenCounts.value().keySet()) {
            if(processedArticle.getTokenCounts().containsKey(queryToken)) {
                int termFrequencyInCurrentDocument = processedArticle.getTokenCounts().get(queryToken);
                int totalTermFrequencyInCorpus = globalTokenCountMap.value().get(queryToken);
                int currentDocumentLength = processedArticle.getTotalTokenCount();
                double currentScore = DPHScorer.getDPHScore((short)termFrequencyInCurrentDocument, totalTermFrequencyInCorpus, currentDocumentLength, averageTokenCountPerDocument, totalDocumentsInCorpus);
                if (!Double.isNaN(currentScore)) {
                    score += currentScore * queryTokenCounts.value().get(queryToken);
                }
            }
        }

        QueryResult result = new QueryResult(processedArticle, score);
        return result;
    }
}
