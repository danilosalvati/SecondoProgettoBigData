package it.uniroma3.bigDataProject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

public class ProlificYearsAnalysis {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("ProlificYearsSpark");
		SparkContext sc = new SparkContext(sparkConf);

		HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc);

		ProlificYearsAnalysis allAnalysis = new ProlificYearsAnalysis();
		/* Creo le tabelle */
		allAnalysis.loadTables(sqlContext);
		/* Faccio partire tutte le analisi necessarie */
		allAnalysis.ProlificYears(sqlContext);
		/* Cancello le tabelle create */
		allAnalysis.deleteTables(sqlContext);
	}

	/**
	 * Metodo di supporto che carica tutte le tabelle in spark
	 */
	private void loadTables(HiveContext sqlContext) {

		/* Estraggo i film */
		sqlContext
				.sql("CREATE TABLE IF NOT EXISTS movies (title STRING, year STRING) "
						+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'");
		sqlContext
				.sql("LOAD DATA INPATH '/input/moviesENDVALUE.list' OVERWRITE INTO TABLE movies");

	}

	private void ProlificYears(HiveContext sqlContext) {

		Row[] results = sqlContext.sql(
				"SELECT year, COUNT(*) as numFilms " + "FROM movies "
						+ "GROUP BY year " + "ORDER BY numFilms DESC "
						+ "LIMIT 1").collect();

		try {
			File folder = new File("Result/");
			folder.mkdir();
			PrintWriter out = new PrintWriter("Result/ProlificYears.txt");
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

		sqlContext.sql("DROP TABLE movies");

	}
}
