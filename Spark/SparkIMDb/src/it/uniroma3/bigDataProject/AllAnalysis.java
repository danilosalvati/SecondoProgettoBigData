package it.uniroma3.bigDataProject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

public class AllAnalysis {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("AllAnalysisSpark");
		SparkContext sc = new SparkContext(sparkConf);

		HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc);

		AllAnalysis allAnalysis = new AllAnalysis();
		/* Creo le tabelle */
		allAnalysis.loadTables(sqlContext);
		/* Faccio partire tutte le analisi necessarie */
		allAnalysis.ProlificActors(sqlContext);
		allAnalysis.ProlificYears(sqlContext);
		allAnalysis.BestActors(sqlContext);
		allAnalysis.BestDirectors(sqlContext);
		allAnalysis.BestCountries(sqlContext);
		allAnalysis.BestProducers(sqlContext);
		allAnalysis.FilmPerYearPerNations(sqlContext);
		allAnalysis.BestMoviesQuotes(sqlContext);
		allAnalysis.BestMoviesKeywords(sqlContext);
		allAnalysis.BestMoviesGenres(sqlContext);
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

		/* Estraggo le nazioni di produzione dei film */
		sqlContext
				.sql("CREATE TABLE IF NOT EXISTS countries (title STRING, nation STRING) "
						+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'");
		sqlContext
				.sql("LOAD DATA INPATH '/input/countriesENDVALUE.list' OVERWRITE INTO TABLE countries");

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

		/* Estraggo i film */
		sqlContext
				.sql("CREATE TABLE IF NOT EXISTS movies (title STRING, year STRING) "
						+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'");
		sqlContext
				.sql("LOAD DATA INPATH '/input/moviesENDVALUE.list' OVERWRITE INTO TABLE movies");

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

		/* Carico la tabella delle keywords */
		sqlContext
				.sql("CREATE TABLE IF NOT EXISTS keywords (film STRING, keyword STRING) "
						+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'");
		sqlContext
				.sql("LOAD DATA INPATH '/input/keywordsENDVALUE.list' OVERWRITE INTO TABLE keywords");

		/* Carico la tabella dei generi */

		sqlContext
				.sql("CREATE TABLE IF NOT EXISTS genres (film STRING, genre STRING) "
						+ "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'");
		sqlContext
				.sql("LOAD DATA INPATH '/input/genresENDVALUE.list' OVERWRITE INTO TABLE genres");

	}

	private void ProlificActors(HiveContext sqlContext) {

		Row[] results = sqlContext.sql(
				"SELECT name, size(filmArray) as numFilm " + "FROM all_actors "
						+ "ORDER BY numFilm DESC " + "LIMIT 10").collect();

		try {
			File folder = new File("Result/");
			folder.mkdir();
			PrintWriter out = new PrintWriter("Result/ProlificActors.txt");
			for (Row result : results) {
				out.println(result.getString(0) + "\t" + result.getInt(1));
			}

			out.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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

	private void BestCountries(HiveContext sqlContext) {

		Row[] results = sqlContext.sql(
				"SELECT nation, COUNT(*) as numFilms "
						+ "FROM ratings, countries "
						+ "WHERE ratings.title=countries.title "
						+ "GROUP BY nation " + "ORDER BY numFilms DESC")
				.collect();

		try {
			File folder = new File("Result/");
			folder.mkdir();
			PrintWriter out = new PrintWriter("Result/BestCountries.txt");
			for (Row result : results) {
				out.println(result.getString(0) + "\t" + result.getLong(1));
			}

			out.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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

		sqlContext.sql("DROP VIEW all_actors");
		sqlContext.sql("DROP TABLE actors");
		sqlContext.sql("DROP TABLE actresses");
		sqlContext.sql("DROP TABLE countries");
		sqlContext.sql("DROP TABLE directors");
		sqlContext.sql("DROP TABLE movies");
		sqlContext.sql("DROP TABLE producers");
		sqlContext.sql("DROP TABLE quotes");
		sqlContext.sql("DROP TABLE ratings");
		sqlContext.sql("DROP TABLE keywords");
		sqlContext.sql("DROP TABLE genres");

	}
}
