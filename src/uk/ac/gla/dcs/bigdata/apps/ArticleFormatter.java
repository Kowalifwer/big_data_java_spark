package uk.ac.gla.dcs.bigdata.apps;
import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.util.LongAccumulator;

/**
 * A custom Spark map function that formats a news article into a processed article with token counts and updates two accumulators for global token counts.
 * @author Artem, Roman
 */

public class ArticleFormatter implements MapFunction<NewsArticle,ProcessedArticle> {
    private static final long serialVersionUID = -484810270156328326L;
    LongAccumulator tokenCountAccumulator;
    CountMapAccumulator tokenCountsMapAccumulator;

    /**
     * Constructs an article formatter with two accumulators as parameters.
     * @param tokenCountAccumulator an accumulator for the total number of tokens in all articles
     * @param tokenCountsMapAccumulator an accumulator for the number of articles in which a token occurs (0 or 1)
     */
    public ArticleFormatter(LongAccumulator tokenCountAccumulator, CountMapAccumulator tokenCountsMapAccumulator) {
        this.tokenCountAccumulator = tokenCountAccumulator;
        this.tokenCountsMapAccumulator = tokenCountsMapAccumulator;
    }

    /**
     * Formats a news article into a processed article with token counts and updates two accumulators for global token counts.
     * @param article the news article to be formatted
     * @return a ProcessedArticle with token counts
     * @throws Exception if any error occurs during formatting
     */
    @Override
    public ProcessedArticle call(NewsArticle article) throws Exception {
        TextPreProcessor processor = new TextPreProcessor();
        StringBuilder articleText = new StringBuilder();
        // add the title to the article text
        if (article.getTitle() != null) {
            articleText.append(article.getTitle());
        }
        // add the non-empty, paragraph subtype contents to the article text
        for (ContentItem item : article.getContents()) {
            if (item != null && item.getSubtype() != null) {
                if (item.getSubtype().equals("paragraph")) {
                    articleText.append(item.getContent());
                }
            }
        }

        //obtain a list of tokens from the article text
        List<String> tokens = processor.process(articleText.toString());

        //create a map to store the token counts for this article
        Map<String, Integer> tokenCounts = new HashMap<String, Integer>();

        //create a separate map to store the binary token counts, i.e. the number of articles in which a token occurs
        HashMap<String, Integer> tokenCountsBinary = new HashMap<String, Integer>();

        //get the number of tokens in this article
        int totalTokenCount = tokens.size();
        
        //update the total token count accumulator with the number of tokens in this article
        tokenCountAccumulator.add(totalTokenCount);
        
        //loop over tokens, for each unique token, count the number of occurences
        for (String token : tokens) {
            if (tokenCounts.containsKey(token)) {
                //if the token already exists in the local map, increment its count by one
                tokenCounts.put(token, tokenCounts.get(token) + 1);
            } else {
                //if the token is new to the local map, set its count to one and add it to the binary map as well
                tokenCounts.put(token, 1);
                tokenCountsBinary.put(token, 1);
            }
        }
        //update the number of articles in which a token occurs accumulator
        tokenCountsMapAccumulator.add(tokenCountsBinary);
        
        //create a processed article with the token counts map and total token count
        ProcessedArticle processed_article = new ProcessedArticle(article, tokenCounts, totalTokenCount);
        return processed_article;
    }
}
