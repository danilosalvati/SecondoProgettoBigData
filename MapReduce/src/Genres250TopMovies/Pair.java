package Genres250TopMovies;

/**
 * Questa classe modella una coppia di valori (String, Integer) rappresentanti
 * la coppia genere e numero di film
 *
 */
public class Pair {
	public String genre;
	public Integer movies;

	public Pair(String genre, Integer movies) {
		this.genre = genre;
		this.movies = movies;
	}
};