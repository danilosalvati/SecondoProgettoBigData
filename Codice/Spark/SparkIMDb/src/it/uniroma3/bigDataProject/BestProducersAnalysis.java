package it.uniroma3.bigDataProject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

public class BestProducersAnalysis {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("BestProducersSpark");
		SparkContext sc = new SparkContext(sparkConf);

		HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc);

		BestProducersAnalysis allAnalysis = new BestProducersAnalysis();
		/* Creo le tabelle */
		allAnalysis.loadTables(sqlContext);
		/* Faccio partire tutte le analisi necessarie */
		allAnalysis.BestProducers(sqlContext);
		/* Cancello le tabelle create */
		allAnalysis.deleteTables(sqlContext);
	}

	/**
	 * Metodo di supporto che carica tutte le tabelle in spark
	 */
	private void loadTables(HiveContext sqlContext) {

		/* Carico la tabella dei produttori */
		sqlContext
				.sql("CREATE TABLE IF NOT EXISTS producersRAW (producer STRING, rowFilm STRING) "
						+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'");
		sqlContext
				.sql("LOAD DATA INPATH '/input/producersENDVALUE.list' OVERWRITE INTO TABLE producersRAW");
		sqlContext
				.sql("CREATE TABLE IF NOT EXISTS producers (producer STRING, filmArray ARRAY<STRING>)");
		sqlContext
				.sql("INSERT INTO TABLE producers "
						+ "SELECT producer, filmArray "
						+ "FROM (SELECT producer, split(rowFilm,'<ENDVALUE>') as filmArray FROM producersRAW) as producers2");
		sqlContext.sql("DROP TABLE producersRAW");

		/* Carico la tabella dei ratings */
		sqlContext
				.sql("CREATE TABLE IF NOT EXISTS ratings (distribution STRING, votes STRING, rank STRING, title STRING) "
						+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'");
		sqlContext
				.sql("LOAD DATA INPATH '/input/top250movies.list' OVERWRITE INTO TABLE ratings");

	}

	private void BestProducers(HiveContext sqlContext) {

		sqlContext.sql("add jar ArrayContainsSubstringUDF.jar");
		sqlContext
				.sql("CREATE TEMPORARY FUNCTION array_contains_substring AS 'it.uniroma3.bigDataProject.ArrayContainsSubstringUDF'");

		Row[] results = sqlContext.sql(
				"SELECT producer, COUNT(*) as numFilms "
						+ "FROM ratings, producers "
						+ "WHERE array_contains_substring(title,filmArray) "
						+ "GROUP BY producer " + "ORDER BY numFilms DESC")
				.collect();

		try {
			File folder = new File("Result/");
			folder.mkdir();
			PrintWriter out = new PrintWriter("Result/BestProducers.txt");
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

		sqlContext.sql("DROP TABLE producers");
		sqlContext.sql("DROP TABLE ratings");

	}
}
