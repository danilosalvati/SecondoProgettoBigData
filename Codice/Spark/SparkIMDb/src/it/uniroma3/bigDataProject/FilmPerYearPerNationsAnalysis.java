package it.uniroma3.bigDataProject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

public class FilmPerYearPerNationsAnalysis {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf()
				.setAppName("FilmPerYearPerNationsSpark");
		SparkContext sc = new SparkContext(sparkConf);

		HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc);

		FilmPerYearPerNationsAnalysis allAnalysis = new FilmPerYearPerNationsAnalysis();
		/* Creo le tabelle */
		allAnalysis.loadTables(sqlContext);
		/* Faccio partire tutte le analisi necessarie */
		allAnalysis.FilmPerYearPerNations(sqlContext);
		/* Cancello le tabelle create */
		allAnalysis.deleteTables(sqlContext);
	}

	/**
	 * Metodo di supporto che carica tutte le tabelle in spark
	 */
	private void loadTables(HiveContext sqlContext) {

		/* Estraggo le nazioni di produzione dei film */
		sqlContext
				.sql("CREATE TABLE IF NOT EXISTS countries (title STRING, nation STRING) "
						+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'");
		sqlContext
				.sql("LOAD DATA INPATH '/input/countriesENDVALUE.list' OVERWRITE INTO TABLE countries");

		/* Estraggo i film */
		sqlContext
				.sql("CREATE TABLE IF NOT EXISTS movies (title STRING, year STRING) "
						+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'");
		sqlContext
				.sql("LOAD DATA INPATH '/input/moviesENDVALUE.list' OVERWRITE INTO TABLE movies");

	}

	private void FilmPerYearPerNations(HiveContext sqlContext) {

		Row[] results = sqlContext.sql(
				"SELECT nation, year, COUNT(*) as numFilms "
						+ "FROM movies, countries "
						+ "WHERE movies.title=countries.title "
						+ "GROUP BY nation, year").collect();

		try {
			File folder = new File("Result/");
			folder.mkdir();
			PrintWriter out = new PrintWriter(
					"Result/FilmPerYearPerNations.txt");
			for (Row result : results) {
				out.println(result.getString(0) + "\t" + result.getString(1)
						+ "\t" + result.getLong(2));
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

		sqlContext.sql("DROP TABLE countries");
		sqlContext.sql("DROP TABLE movies");

	}
}
