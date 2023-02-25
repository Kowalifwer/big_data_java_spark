package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;

import org.apache.spark.util.LongAccumulator;

/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {

	//create new list classed strings
	public static List<String> prints = new ArrayList<String>();

	public static void print(Object... objs) {
		//add to prints
		StringBuilder sb = new StringBuilder();
		for(Object obj : objs) {
			sb.append(obj.toString());
			//if not last object, add comma
			if(obj != objs[objs.length-1])
				sb.append(" ");
		}
		prints.add(sb.toString());
	}

	public static void finalize_print() {	
		//loop over prints, print each on new line
		System.out.println("---------------PRINTS---------------");
		for (String s : prints) {
			System.out.println(s);
		}
		System.out.println("-------------------------------------");
	}
	
	public static void main(String[] args) {
		
		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[8]"; // default is local mode with two executors
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
			.setMaster(sparkMasterDef)
			.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
		
		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		// if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
        if (queryFile==null) queryFile = "data/queries_custom.list"; // default is a sample with 3 queries

		
		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		// if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix.json"; // default is a sample of 600,000 news articles
		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		
		// Close the spark session
		spark.close();
		
		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			
			// We have set of output rankings, lets write to disk
			
			// Create a new folder 
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();
			
			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}
		
		finalize_print();
	}

    //for query in all existing queries FOR LOOP
        	// for document in processedArticles SPARK LOOP -to calculate relevancy scores for all the documents
				//for query token in query -> fetch scores from map NORMAL FOR
                //sum all the token stuff
			//-> output map from QUERY to DOCUMENT RELEVANCY/SCORE
        
        
                //first we have query -> all documents map. Each document will store associated DPH score.

    public static List<NewsArticle> processQuery(SparkSession spark, Query query, Dataset<ProcessedArticle> processedNews, double averageTokenCountPerDocument, Broadcast<Map<String, Integer>> corpusTokenCountMap, long totalDocsInCorpusBroadcast, int nResults, boolean verbose) {
		long startTime = System.currentTimeMillis();

        List<String> queryTerms = query.getQueryTerms();
        short[] queryTermCounts = query.getQueryTermCounts();

        Map<String, Short> queryTokenCountMap = new HashMap<String, Short>();
        for (int i = 0; i < query.getQueryTerms().size(); i++) {
            queryTokenCountMap.put(queryTerms.get(i), queryTermCounts[i]);
        }

        Broadcast<Map<String, Short>> queryTokenCountMapBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queryTokenCountMap);
        
        QueryResultFormatter resultsFormatter = new QueryResultFormatter(averageTokenCountPerDocument, corpusTokenCountMap, queryTokenCountMapBroadcast, totalDocsInCorpusBroadcast);

        Dataset<QueryResult> queryResults = processedNews.map(resultsFormatter, Encoders.bean(QueryResult.class));
		Dataset<QueryResult> orderedQueryResults = queryResults.orderBy(functions.col("score").desc());
        //orderedQueryResults limit to first max of nResults*10 and collect as list

        List<QueryResult> orderedQueryResultsList = orderedQueryResults.limit(nResults*10).collectAsList();
        List<NewsArticle> bestMatchesArticles = new ArrayList<NewsArticle>();

		final double TITLE_SIMILARITY_TRESHOLD = 0.5;
        int duplicate_counter = 0;
        double best_score = 0;
        double worst_score = Double.MAX_VALUE;
    
		for (QueryResult current: orderedQueryResultsList) {
            if (bestMatchesArticles.size() < nResults) {
                NewsArticle article = current.getProcessedArticle().getNewsArticle();

                if (current.getScore() > best_score) {
                    best_score = current.getScore();
                }
                if (current.getScore() < worst_score) {
                    worst_score = current.getScore();
                }

                boolean found = false;
                if (article.getTitle() != null) {
                    for (NewsArticle comparingNewsArticle : bestMatchesArticles) {
                        if(comparingNewsArticle.getTitle() != null && TextDistanceCalculator.similarity(article.getTitle(), comparingNewsArticle.getTitle()) < TITLE_SIMILARITY_TRESHOLD) {
                            found = true;
                            duplicate_counter++;
                            break;
                        }
                    }
                }

                if(!found) {
                    bestMatchesArticles.add(article);
                }
            }
            else {
                break;
            }
        }
		if (verbose) {
        	best_score = Math.round(best_score * 100.0) / 100.0;
        	worst_score = Math.round(worst_score * 100.0) / 100.0;
            print("Executing query: \"" + query.getOriginalQuery() + "\"", "Number of duplicates removed: " + duplicate_counter, "Best score: " + best_score, "Worst score: " + worst_score, "Time taken: " + (System.currentTimeMillis() - startTime) + "ms");
			for(NewsArticle article : bestMatchesArticles) {
				String title = "<no title>";
				if (article.getTitle() != null) {
					title = article.getTitle();
				}
				print("\t", title);
			}
			print("\n");
        }
        return bestMatchesArticles;
    }
	
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article

		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle

		LongAccumulator tokenCountAccumulator = spark.sparkContext().longAccumulator();
		MapAccumulator tokenCountMapAccumulator = new MapAccumulator();
        spark.sparkContext().register(tokenCountMapAccumulator, "tokenCountMapAccumulator");
        
		ArticleFormatter articleFormatter = new ArticleFormatter(tokenCountAccumulator, tokenCountMapAccumulator);
		Dataset<ProcessedArticle> proccessedNews = news.map(articleFormatter, Encoders.bean(ProcessedArticle.class));
        
        long totalDocsInCorups = proccessedNews.count();
        double averageTokenCountPerDocument = (double)tokenCountAccumulator.value() / totalDocsInCorups;
        
        Broadcast<Map<String, Integer>> corpusTokenCountMapBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(tokenCountMapAccumulator.value());

        //go over all queries, and for each query, run the processQuery function
        for (Query query : queries.collectAsList()) {
            List<NewsArticle> queryResult = processQuery(spark, query, proccessedNews, averageTokenCountPerDocument, corpusTokenCountMapBroadcast, totalDocsInCorups, 10, true);
        }

		return null;
	}
	
	
}
