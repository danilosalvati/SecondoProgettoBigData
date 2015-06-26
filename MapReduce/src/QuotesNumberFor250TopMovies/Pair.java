package QuotesNumberFor250TopMovies;

/**
 * Questa classe modella una coppia di valori (String, Integer) rappresentanti
 * la coppia film e numero di citazioni
 *
 */
public class Pair {
	public String movie;
	public Integer quotes;

	public Pair(String movie, Integer quotes) {
		this.movie = movie;
		this.quotes = quotes;
	}
};