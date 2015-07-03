package it.uniroma3.bigDataProject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

public class BestDirectorsAnalysis {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("BestDirectorsSpark");
		SparkContext sc = new SparkContext(sparkConf);

		HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc);

		BestDirectorsAnalysis allAnalysis = new BestDirectorsAnalysis();
		/* Creo le tabelle */
		allAnalysis.loadTables(sqlContext);
		/* Faccio partire tutte le analisi necessarie */
		allAnalysis.BestDirectors(sqlContext);
		/* Cancello le tabelle create */
		allAnalysis.deleteTables(sqlContext);
	}

	/**
	 * Metodo di supporto che carica tutte le tabelle in spark
	 */
	private void loadTables(HiveContext sqlContext) {

		/* Carico la tabella dei registi */
		sqlContext
				.sql("CREATE TABLE IF NOT EXISTS directorsRAW (director STRING, rowFilm STRING) "
						+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'");
		sqlContext
				.sql("LOAD DATA INPATH '/input/directorsENDVALUE.list' OVERWRITE INTO TABLE directorsRAW");
		sqlContext
				.sql("CREATE TABLE IF NOT EXISTS directors (director STRING, filmArray ARRAY<STRING>)");
		sqlContext
				.sql("INSERT INTO TABLE directors "
						+ "SELECT director, filmArray "
						+ "FROM (SELECT director, split(rowFilm,'<ENDVALUE>') as filmArray FROM directorsRAW) as directors2");
		sqlContext.sql("DROP TABLE directorsRAW");

		/* Carico la tabella dei ratings */
		sqlContext
				.sql("CREATE TABLE IF NOT EXISTS ratings (distribution STRING, votes STRING, rank STRING, title STRING) "
						+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'");
		sqlContext
				.sql("LOAD DATA INPATH '/input/top250movies.list' OVERWRITE INTO TABLE ratings");

	}

	private void BestDirectors(HiveContext sqlContext) {

		sqlContext.sql("add jar ArrayContainsSubstringUDF.jar");
		sqlContext
				.sql("CREATE TEMPORARY FUNCTION array_contains_substring AS 'it.uniroma3.bigDataProject.ArrayContainsSubstringUDF'");

		Row[] results = sqlContext.sql(
				"SELECT director, COUNT(*) as numFilms "
						+ "FROM directors, ratings "
						+ "WHERE array_contains_substring(title,filmArray) "
						+ "GROUP BY director " + "ORDER BY numFilms DESC")
				.collect();

		try {
			File folder = new File("Result/");
			folder.mkdir();
			PrintWriter out = new PrintWriter("Result/BestDirectors.txt");
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

		sqlContext.sql("DROP TABLE directors");
		sqlContext.sql("DROP TABLE ratings");

	}
}
