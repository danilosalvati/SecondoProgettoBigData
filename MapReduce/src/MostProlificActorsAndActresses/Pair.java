package MostProlificActorsAndActresses;

/**
 * Questa classe modella una coppia di valori (String, Integer) rappresentanti
 * l'attore/attrice ed il numero di film
 *
 */
public class Pair {
	
	public String actor;
	public Integer moviesNumber;

	public Pair(String actor, Integer moviesNumber) {
		this.actor = actor;
		this.moviesNumber = moviesNumber;
	}
};