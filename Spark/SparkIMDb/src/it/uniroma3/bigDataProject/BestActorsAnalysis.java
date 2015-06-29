package it.uniroma3.bigDataProject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

public class BestActorsAnalysis {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("BestActorsSpark");
		SparkContext sc = new SparkContext(sparkConf);

		HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc);

		BestActorsAnalysis allAnalysis = new BestActorsAnalysis();
		/* Creo le tabelle */
		allAnalysis.loadTables(sqlContext);
		/* Faccio partire tutte le analisi necessarie */
		allAnalysis.BestActors(sqlContext);
		/* Cancello le tabelle create */
		allAnalysis.deleteTables(sqlContext);
	}

	/**
	 * Metodo di supporto che carica tutte le tabelle in spark
	 */
	private void loadTables(HiveContext sqlContext) {

		/* Carico la tabella degli attori */
		sqlContext
				.sql("CREATE TABLE IF NOT EXISTS actorsRAW (actor STRING, rowFilm STRING) "
						+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'");
		sqlContext
				.sql("LOAD DATA INPATH '/input/actorsENDVALUE.list' OVERWRITE INTO TABLE actorsRAW");
		sqlContext
				.sql("CREATE TABLE IF NOT EXISTS actors (actor STRING, filmArray ARRAY<STRING>)");
		sqlContext
				.sql("INSERT INTO TABLE actors "
						+ "SELECT actor, filmArray "
						+ "FROM (SELECT actor, split(rowFilm,'<ENDVALUE>') as filmArray FROM actorsRAW) as actors2");
		sqlContext.sql("DROP TABLE actorsRaw");

		/* Carico la tabella delle attrici */
		sqlContext
				.sql("CREATE TABLE IF NOT EXISTS actressesRAW (actress STRING, rowFilm STRING) "
						+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'");
		sqlContext
				.sql("LOAD DATA INPATH '/input/actressesENDVALUE.list' OVERWRITE INTO TABLE actressesRAW");
		sqlContext
				.sql("CREATE TABLE IF NOT EXISTS actresses (actress STRING, filmArray ARRAY<STRING>)");
		sqlContext
				.sql("INSERT INTO TABLE actresses "
						+ "SELECT actress, filmArray "
						+ "FROM (SELECT actress, split(rowFilm,'<ENDVALUE>') as filmArray FROM actressesRAW) as actresses2");
		sqlContext.sql("DROP TABLE actressesRaw");

		/* Unisco le tabelle degli attori e delle attrici */
		sqlContext.sql("CREATE VIEW all_actors (name, filmArray) AS "
				+ "SELECT actor as name, filmArray " + "FROM actors "
				+ "UNION ALL " + "SELECT actress as name, filmArray "
				+ "FROM actresses");

		/* Carico la tabella dei ratings */
		sqlContext
				.sql("CREATE TABLE IF NOT EXISTS ratings (distribution STRING, votes STRING, rank STRING, title STRING) "
						+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'");
		sqlContext
				.sql("LOAD DATA INPATH '/input/top250movies.list' OVERWRITE INTO TABLE ratings");

	}

	private void BestActors(HiveContext sqlContext) {

		sqlContext.sql("add jar ArrayContainsSubstringUDF.jar");
		sqlContext
				.sql("CREATE TEMPORARY FUNCTION array_contains_substring AS 'it.uniroma3.bigDataProject.ArrayContainsSubstringUDF'");

		Row[] results = sqlContext.sql(
				"SELECT name, COUNT(*) as numFilms "
						+ "FROM all_actors, ratings "
						+ "WHERE array_contains_substring(title,filmArray) "
						+ "GROUP BY name " + "ORDER BY numFilms DESC")
				.collect();

		try {
			File folder = new File("Result/");
			folder.mkdir();
			PrintWriter out = new PrintWriter("Result/BestActors.txt");
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

		sqlContext.sql("DROP VIEW all_actors");
		sqlContext.sql("DROP TABLE actors");
		sqlContext.sql("DROP TABLE actresses");
		sqlContext.sql("DROP TABLE ratings");

	}
}
