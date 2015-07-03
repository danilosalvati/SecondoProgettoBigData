package it.uniroma3.bigDataProject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

public class BestMoviesKeywordsAnalysis {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf()
				.setAppName("BestMoviesKeywordsSpark");
		SparkContext sc = new SparkContext(sparkConf);

		HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc);

		BestMoviesKeywordsAnalysis allAnalysis = new BestMoviesKeywordsAnalysis();
		/* Creo le tabelle */
		allAnalysis.loadTables(sqlContext);
		/* Faccio partire tutte le analisi necessarie */
		allAnalysis.BestMoviesKeywords(sqlContext);
		/* Cancello le tabelle create */
		allAnalysis.deleteTables(sqlContext);
	}

	/**
	 * Metodo di supporto che carica tutte le tabelle in spark
	 */
	private void loadTables(HiveContext sqlContext) {

		/* Carico la tabella dei ratings */
		sqlContext
				.sql("CREATE TABLE IF NOT EXISTS ratings (distribution STRING, votes STRING, rank STRING, title STRING) "
						+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'");
		sqlContext
				.sql("LOAD DATA INPATH '/input/top250movies.list' OVERWRITE INTO TABLE ratings");

		/* Carico la tabella delle keywords */
		sqlContext
				.sql("CREATE TABLE IF NOT EXISTS keywords (film STRING, keyword STRING) "
						+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'");
		sqlContext
				.sql("LOAD DATA INPATH '/input/keywordsENDVALUE.list' OVERWRITE INTO TABLE keywords");

	}

	private void BestMoviesKeywords(HiveContext sqlContext) {

		Row[] results = sqlContext.sql(
				"SELECT keyword, COUNT(*) as numFilms "
						+ "FROM ratings, keywords "
						+ "WHERE ratings.title=keywords.film "
						+ "GROUP BY keyword " + "ORDER BY numFilms DESC "
						+ "LIMIT 100").collect();

		try {
			File folder = new File("Result/");
			folder.mkdir();
			PrintWriter out = new PrintWriter("Result/BestMoviesKeywords.txt");
			for (Row result : results) {
				out.println(result.getString(0) + "\t" + result.getLong(1));
			}

			out.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Metodo di supporto che cancella tutte le tabelle caricate in spark
	 */
	private void deleteTables(HiveContext sqlContext) {

		sqlContext.sql("DROP TABLE ratings");
		sqlContext.sql("DROP TABLE keywords");

	}
}
