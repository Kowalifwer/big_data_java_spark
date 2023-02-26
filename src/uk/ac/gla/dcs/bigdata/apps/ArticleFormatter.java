package uk.ac.gla.dcs.bigdata.apps;
import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.util.LongAccumulator;

public class ArticleFormatter implements MapFunction<NewsArticle,ProcessedArticle> {
    private static final long serialVersionUID = -484810270156328326L;

    LongAccumulator tokenCountAccumulator;
    CountMapAccumulator tokenCountsMapAccumulator;

    public ArticleFormatter(LongAccumulator tokenCountAccumulator, CountMapAccumulator tokenCountsMapAccumulator) {
        this.tokenCountAccumulator = tokenCountAccumulator;
        this.tokenCountsMapAccumulator = tokenCountsMapAccumulator;
    }

    @Override
    public ProcessedArticle call(NewsArticle article) throws Exception {
        TextPreProcessor processor = new TextPreProcessor();
        StringBuilder sb = new StringBuilder();
        if (article.getTitle() != null) {
            sb.append(article.getTitle());
        }
        for (ContentItem item : article.getContents()) {
            if (item != null && item.getSubtype() != null) {
                if (item.getSubtype().equals("paragraph")) {
                    sb.append(item.getContent());
                }
            }
        }

        List<String> tokens = processor.process(sb.toString());
        Map<String, Integer> tokenCounts = new HashMap<String, Integer>();
        HashMap<String, Integer> tokenCountsBinary = new HashMap<String, Integer>(); //separate map required for binary counts for the global counts accumulator
        //loop over tokens, for each unique token, count the number of occurences
        int totalTokenCount = tokens.size();
        tokenCountAccumulator.add(totalTokenCount);
        for (String token : tokens) {
            if (tokenCounts.containsKey(token)) {
                tokenCounts.put(token, tokenCounts.get(token) + 1);
            } else {
                tokenCounts.put(token, 1);
                tokenCountsBinary.put(token, 1);
            }
        }
        tokenCountsMapAccumulator.add(tokenCountsBinary);

        ProcessedArticle processed_article = new ProcessedArticle(article, tokenCounts, totalTokenCount);
        return processed_article;
    }
}
