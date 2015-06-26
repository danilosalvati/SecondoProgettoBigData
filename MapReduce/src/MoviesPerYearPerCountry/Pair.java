package MoviesPerYearPerCountry;

/**
 * Questa classe modella una coppia di valori (String, Integer) rappresentanti
 * la coppia nazione e numero di film
 *
 */
public class Pair {
	public String countryAndYear;
	public Integer movies;

	public Pair(String countryAndYear, Integer movies) {
		this.countryAndYear = countryAndYear;
		this.movies = movies;
	}
};