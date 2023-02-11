package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.thirdparty.org.checkerframework.checker.units.qual.C;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

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
		if (sparkMasterDef==null) sparkMasterDef = "local[2]"; // default is local mode with two executors
		
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
		
		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		
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
	
	
	
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article
		
		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle
		
		// Broadcast<TextPreProcessor> broadcastPreProcessor= JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(new TextPreProcessor());
		// ArticleFormatter xd = new ArticleFormatter(broadcastPreProcessor);

		Dataset<ProcessedArticle> proccessedNews = news.map(new ArticleFormatter(), Encoders.bean(ProcessedArticle.class));
		//at this point we should have the following:
		//- short termFrequencyInCurrentDocument,
		// int totalTermFrequencyInCorpus,
		// int currentDocumentLength,

		print(proccessedNews.first());
		// double averageDocumentLengthInCorpus,
		// long totalDocsInCorpus)


		//



		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		//----------------------------------------------------------------
		//HERE
		//set of queries -> query -> [10 documents]
		//each doc processed, remove stopwords etc..
		//For this exercise, you only need to use the ‘id’, ‘title’ and ‘contents’ fields. from NEWSARTICLE
		return null; // replace this with the the list of DocumentRanking output by your topology
	}
	
	
}