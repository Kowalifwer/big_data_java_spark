package uk.ac.gla.dcs.bigdata.apps;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;

import java.util.Map;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;

/**
 * A custom Spark map function that formats a processed news article into a RankedResult with a DPH score for a given query.
 * @author Artem, Roman
 */
public class RankedResultFormatter implements MapFunction<ProcessedArticle, RankedResult> {
    long totalDocumentsInCorpus;
    double averageTokenCountPerDocument;
    Broadcast<Map<String, Short>> queryTokenCounts;
    Broadcast<Map<String, Integer>> globalTokenCountMap;

    /**
     * Constructs a new RankedResultFormatter object with the given parameters.
     * @param totalDocumentsInCorpus The total number of documents in the corpus
     * @param averageTokenCountPerDocument The average number of tokens per document in the corpus
     * @param globalTokenCountMap A broadcast variable that maps a token to then number of articles in which the token occurs
     * @param queryTokenCounts A broadcast variable that contains the query token counts for each term
     */
    public RankedResultFormatter(long totalDocumentsInCorpus, double averageTokenCountPerDocument, Broadcast<Map<String, Short>> queryTokenCounts, Broadcast<Map<String, Integer>> globalTokenCountMap) {
        this.totalDocumentsInCorpus = totalDocumentsInCorpus;
        this.averageTokenCountPerDocument = averageTokenCountPerDocument;
        this.queryTokenCounts = queryTokenCounts;
        this.globalTokenCountMap = globalTokenCountMap;
    }

    /**
     * Applies the map function to a ProcessedArticle object and returns a RankedResult object with a score.
     * The score is computed by summing up the DPH scores for each query token that appears in the document.
     * @param processedArticle The ProcessedArticle object to be mapped
     * @return A RankedResult object with an id, a NewsArticle object and a DPH score
     */
    @Override
    public RankedResult call(ProcessedArticle processedArticle) throws Exception {
        double score = 0;
        //iterate over the query tokens and compute the DPH score for each token
        for(String queryToken : queryTokenCounts.value().keySet()) {
            if(processedArticle.getTokenCounts().containsKey(queryToken)) { //if the query token appears in the document
                int termFrequencyInCurrentDocument = processedArticle.getTokenCounts().get(queryToken);
                int totalTermFrequencyInCorpus = globalTokenCountMap.value().get(queryToken);
                int currentDocumentLength = processedArticle.getTotalTokenCount();
                double currentScore = DPHScorer.getDPHScore((short)termFrequencyInCurrentDocument, totalTermFrequencyInCorpus, currentDocumentLength, averageTokenCountPerDocument, totalDocumentsInCorpus);
                if (!Double.isNaN(currentScore)) {
                    //add the DPH score of the query term, to the total score. Make sure to multiply the score by the query term count
                    score += currentScore * queryTokenCounts.value().get(queryToken);
                }
            }
        }
        // return a RankedResult object with the id, the NewsArticle object and the score
        RankedResult result = new RankedResult(processedArticle.getNewsArticle().getId(), processedArticle.getNewsArticle(), score);
        return result;
    }
}
