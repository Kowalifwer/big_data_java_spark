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
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
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
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
        // UNCOMMENT TO USE CUSTOM QUERY LISTif (queryFile==null) queryFile = "data/queries_custom.list";
		
		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		// if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix.json"; // default is a sample of 600,000 news articles
		
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

    /**
     * Processes a query and returns a list of top nResults ranked results, based on the DPH scores of the query terms w.r.t. the documents.
     * @param spark The SparkSession object
     * @param query The Query object containing the query terms and counts
     * @param processedArticles The Dataset of ProcessedArticle objects containing the articles to be searched, and their token metrics.
     * @param totalDocsInCorpusBroadcast The total number of documents in the corpus
     * @param averageTokenCountPerDocument The average number of tokens per document in the corpus
     * @param corpusTokenCountMap The broadcast variable containing a map from token to the number of articles in which then token occurs (in the corpus)
     * @param nResults The maximum number of results to return
     * @param verbose A boolean flag indicating whether to print a verbose report for each query, or not
     * @return A list of RankedResult objects ordered by descending score
     */
    public static List<RankedResult> processQuery(SparkSession spark, Query query, Dataset<ProcessedArticle> processedArticles, long totalDocsInCorpusBroadcast, double averageTokenCountPerDocument, Broadcast<Map<String, Integer>> corpusTokenCountMap, int nResults, boolean verbose) {
		long startTime = System.currentTimeMillis();

        //get the list of query terms and the respective array of counts
        List<String> queryTerms = query.getQueryTerms();
        short[] queryTermCounts = query.getQueryTermCounts();

        //create a map from query term to query term count
        Map<String, Short> queryTokenCountMap = new HashMap<String, Short>();
        for (int i = 0; i < queryTerms.size(); i++) {
            queryTokenCountMap.put(queryTerms.get(i), queryTermCounts[i]);
        }
        //broadcast the query token count map
        Broadcast<Map<String, Short>> queryTokenCountMapBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queryTokenCountMap);
        
        //instantiate the ranked results formatter with 2 variables and 2 broadcast variables. More details in the class constructor documentation below.
        RankedResultFormatter resultsFormatter = new RankedResultFormatter(totalDocsInCorpusBroadcast, averageTokenCountPerDocument, queryTokenCountMapBroadcast, corpusTokenCountMap);
        // map the processed articles to the ranked results, using the instantiated formatter
        Dataset<RankedResult> queryResults = processedArticles.map(resultsFormatter, Encoders.bean(RankedResult.class));
		// order the results by descending order of score
        Dataset<RankedResult> orderedQueryResults = queryResults.orderBy(functions.col("score").desc());
        
        //orderedQueryResults limit to first max of nResults*10 and collect it into a list, so that we can work with it.
        //limiting to nResults*10 is to ensure that we have enough results to filter out duplicates, and to prevent loading the whole dataset into memory.
        List<RankedResult> orderedQueryResultsList = orderedQueryResults.limit(nResults*10).collectAsList();
        //instantiate empty list to store the best, non duplicate results
        List<RankedResult> bestRankedResults = new ArrayList<RankedResult>();

		final double TITLE_SIMILARITY_TRESHOLD = 0.5; //thereshold for 2 titles to be considered similar, and regarded as duplicates
        int duplicate_counter = 0; //counter for the number of duplicates found (for verbose report)
        double best_score = 0; //best score found (for verbose report)
        double worst_score = Double.MAX_VALUE; //worst score found (for verbose report)

        //iterate over the ordered results list, and add the best results to the bestRankedResults list, while filtering out title duplicates
		for (RankedResult current: orderedQueryResultsList) {
            if (bestRankedResults.size() < nResults) {
                NewsArticle article = current.getArticle();

                if (current.getScore() > best_score) {
                    best_score = current.getScore();
                }
                if (current.getScore() < worst_score) {
                    worst_score = current.getScore();
                }
                
                boolean hasDuplicate = false; //flag to indicate whether the current result has a duplicate title in the current bestRankedResults list, or not
                
                //if the current result has a title, and it is similar to any of the titles in the bestRankedResults list, then it is a duplicate
                if (article.getTitle() != null) {
                    for (RankedResult rankedResult : bestRankedResults) {
                        NewsArticle comparingArticle = rankedResult.getArticle();
                        if(comparingArticle.getTitle() != null && TextDistanceCalculator.similarity(article.getTitle(), comparingArticle.getTitle()) < TITLE_SIMILARITY_TRESHOLD) {
                            hasDuplicate = true;
                            duplicate_counter++;
                            break;
                        }
                    }
                }

                if(!hasDuplicate) {
                    bestRankedResults.add(current);
                }
            }
            else {
                break;
            }
        }

        //code that handles the formatting for the verbose report
		if (verbose) {
        	best_score = Math.round(best_score * 100.0) / 100.0;
        	worst_score = Math.round(worst_score * 100.0) / 100.0;
            print("Executing query: \"" + query.getOriginalQuery() + "\"", "Number of duplicates removed: " + duplicate_counter, "Best score: " + best_score, "Worst score: " + worst_score, "Time taken: " + (System.currentTimeMillis() - startTime) + "ms");
			for(RankedResult rankedResult : bestRankedResults) {
				String title = "<no title>";
				if (rankedResult.getArticle().getTitle() != null) {
					title = rankedResult.getArticle().getTitle();
				}
				print("\t", title);
			}
			print("\n");
        }
        return bestRankedResults;
    }
	
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article

		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle

        // Initialize our 2 accumulators to be passed into articleFormatter. For more details, see articleFormatter class constructor java doc.
		LongAccumulator tokenCountAccumulator = spark.sparkContext().longAccumulator();
		CountMapAccumulator tokenCountMapAccumulator = new CountMapAccumulator();
        spark.sparkContext().register(tokenCountMapAccumulator, "tokenCountMapAccumulator");
        
        // Process the news articles, and return a new dataset of ProcessedArticles
		ArticleFormatter articleFormatter = new ArticleFormatter(tokenCountAccumulator, tokenCountMapAccumulator);
		Dataset<ProcessedArticle> processedArticles = news.map(articleFormatter, Encoders.bean(ProcessedArticle.class));
        
        // Count the total number of documents in the corpus, and calculate the average number of tokens per document
        long totalDocsInCorups = processedArticles.count();
        double averageTokenCountPerDocument = (double)tokenCountAccumulator.value() / totalDocsInCorups;
        
        // Broadcast the variable that maps a token to then number of articles in which the token occurs (across the whole corpus)
        Broadcast<Map<String, Integer>> corpusTokenCountMapBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(tokenCountMapAccumulator.value());

        // Create a list of DocumentRanking objects, which will store the results of each query
        List<DocumentRanking> results = new ArrayList<DocumentRanking>();
        //go over all queries, for each one run the processQuery function that will return a list of RankedResults of size nResults 
        for (Query query : queries.collectAsList()) {
            List<RankedResult> rankedResult = processQuery(spark, query, processedArticles, totalDocsInCorups, averageTokenCountPerDocument, corpusTokenCountMapBroadcast, 10, true);
            results.add(new DocumentRanking(query, rankedResult));
        }

		return results;
	}
	
}
