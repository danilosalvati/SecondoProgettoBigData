package Directors250TopMovies;

/**
 * Questa classe modella una coppia di valori (String, Integer) rappresentanti
 * la coppia attore e rispettivi film
 *
 */
public class Pair {
	public String director;
	public Integer movies;

	public Pair(String director, Integer movies) {
		this.director = director;
		this.movies = movies;
	}
};