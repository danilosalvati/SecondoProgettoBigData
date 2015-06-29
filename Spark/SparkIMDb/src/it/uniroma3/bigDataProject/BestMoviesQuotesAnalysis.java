package it.uniroma3.bigDataProject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

public class BestMoviesQuotesAnalysis {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf()
				.setAppName("BestMoviesQuotesSpark");
		SparkContext sc = new SparkContext(sparkConf);

		HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc);

		BestMoviesQuotesAnalysis allAnalysis = new BestMoviesQuotesAnalysis();
		/* Creo le tabelle */
		allAnalysis.loadTables(sqlContext);
		/* Faccio partire tutte le analisi necessarie */
		allAnalysis.BestMoviesQuotes(sqlContext);
		/* Cancello le tabelle create */
		allAnalysis.deleteTables(sqlContext);
	}

	/**
	 * Metodo di supporto che carica tutte le tabelle in spark
	 */
	private void loadTables(HiveContext sqlContext) {

		/* Carico la tabella delle citazioni */
		sqlContext
				.sql("CREATE TABLE IF NOT EXISTS quotesRAW (film STRING, rowQuotes STRING) "
						+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'");
		sqlContext
				.sql("LOAD DATA INPATH '/input/quotesENDVALUE.list' OVERWRITE INTO TABLE quotesRAW");
		sqlContext
				.sql("CREATE TABLE IF NOT EXISTS quotes (film STRING, quotesArray ARRAY<STRING>)");
		sqlContext
				.sql("INSERT INTO TABLE quotes "
						+ "SELECT film, quotesArray "
						+ "FROM (SELECT film, split(rowQuotes,'<ENDVALUE>') as quotesArray FROM quotesRAW) as quotes2");
		sqlContext.sql("DROP TABLE quotesRaw");

		/* Carico la tabella dei ratings */
		sqlContext
				.sql("CREATE TABLE IF NOT EXISTS ratings (distribution STRING, votes STRING, rank STRING, title STRING) "
						+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'");
		sqlContext
				.sql("LOAD DATA INPATH '/input/top250movies.list' OVERWRITE INTO TABLE ratings");
	}

	private void BestMoviesQuotes(HiveContext sqlContext) {

		Row[] results = sqlContext.sql(
				"SELECT film, size(quotesArray) as numQuotes "
						+ "FROM ratings, quotes "
						+ "WHERE ratings.title=quotes.film "
						+ "ORDER BY numQuotes DESC").collect();

		try {
			File folder = new File("Result/");
			folder.mkdir();
			PrintWriter out = new PrintWriter("Result/BestMoviesQuotes.txt");
			for (Row result : results) {
				out.println(result.getString(0) + "\t" + result.getInt(1));
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
		sqlContext.sql("DROP TABLE movies");
		sqlContext.sql("DROP TABLE quotes");
	}
}
