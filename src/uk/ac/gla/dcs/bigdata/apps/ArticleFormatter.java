package uk.ac.gla.dcs.bigdata.apps;
import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import java.util.List;
import org.apache.spark.broadcast.Broadcast;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.util.LongAccumulator;


public class ArticleFormatter implements MapFunction<NewsArticle,ProcessedArticle> {
    private static final long serialVersionUID = -484810270156328326L;

    //global processor
    // Broadcast<TextPreProcessor> processor;
    LongAccumulator wordCountAccumulator;
    MapAccumulator tokenCountsMapAccumulator;
    // public ArticleFormatter(Broadcast<TextPreProcessor> textProcessor) {
    //     this.processor = textProcessor;
    // }

    public ArticleFormatter(LongAccumulator wordCountAccumulator, MapAccumulator tokenCountsMapAccumulator) {
        this.wordCountAccumulator = wordCountAccumulator;
        this.tokenCountsMapAccumulator = tokenCountsMapAccumulator;
    }
    // {xd: 5, ass: 6, lmao}    
    // {ass:6, dick; 17}.update(next)
    //3: etc..

    //GLOBAL COUNTS DICT
    //aVG DOC LENGTH

    @Override
    public ProcessedArticle call(NewsArticle article) throws Exception {
        TextPreProcessor processor = new TextPreProcessor();
        StringBuilder sb = new StringBuilder();
        if (article.getTitle() != null) {
            sb.append(article.getTitle());
        }
        for (ContentItem item : article.getContents()) {
            if (item.getSubtype() != null) {
                if (item.getSubtype().equals("paragraph")) {
                    sb.append(item.getContent());
                }
            }
        }

        List<String> tokens = processor.process(sb.toString());
        Map<String, Integer> tokenCounts = new HashMap<String, Integer>();
        //loop over tokens, for each unique token, count the number of occurences
        int totalTokenCount = tokens.size();
        wordCountAccumulator.add(totalTokenCount);
        for (String token : tokens) {
            if (tokenCounts.containsKey(token)) {
                tokenCounts.put(token, tokenCounts.get(token) + 1);
            } else {
                tokenCounts.put(token, 1);
            }
        }
        tokenCountsMapAccumulator.add(tokenCounts);


        ProcessedArticle processed_article = new ProcessedArticle(article.getId(), tokenCounts, totalTokenCount);
        return processed_article;
    }
        
}
