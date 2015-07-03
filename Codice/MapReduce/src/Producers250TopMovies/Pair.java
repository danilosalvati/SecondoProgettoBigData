package Producers250TopMovies;

/**
 * Questa classe modella una coppia di valori (String, Integer) rappresentanti
 * la coppia produttore e rispettivi film
 *
 */
public class Pair {
	public String producer;
	public Integer movies;

	public Pair(String producer, Integer movies) {
		this.producer = producer;
		this.movies = movies;
	}
};