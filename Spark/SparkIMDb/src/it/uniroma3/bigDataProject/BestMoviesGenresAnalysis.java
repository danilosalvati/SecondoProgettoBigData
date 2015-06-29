package it.uniroma3.bigDataProject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

public class BestMoviesGenresAnalysis {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("BestMoviesGenresSpark");
		SparkContext sc = new SparkContext(sparkConf);

		HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc);

		BestMoviesGenresAnalysis allAnalysis = new BestMoviesGenresAnalysis();
		/* Creo le tabelle */
		allAnalysis.loadTables(sqlContext);
		/* Faccio partire tutte le analisi necessarie */
		allAnalysis.BestMoviesGenres(sqlContext);
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

		/* Carico la tabella dei generi */

		sqlContext
				.sql("CREATE TABLE IF NOT EXISTS genres (film STRING, genre STRING) "
						+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'");
		sqlContext
				.sql("LOAD DATA INPATH '/input/genresENDVALUE.list' OVERWRITE INTO TABLE genres");

	}

	private void BestMoviesGenres(HiveContext sqlContext) {

		Row[] results = sqlContext.sql(
				"SELECT genre, COUNT(*) as numFilms " + "FROM ratings, genres "
						+ "WHERE ratings.title=genres.film "
						+ "GROUP BY genre " + "ORDER BY numFilms DESC")
				.collect();

		try {
			File folder = new File("Result/");
			folder.mkdir();
			PrintWriter out = new PrintWriter("Result/BestMoviesGenres.txt");
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
		sqlContext.sql("DROP TABLE genres");

	}
}
