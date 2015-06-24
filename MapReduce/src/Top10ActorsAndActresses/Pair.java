package Top10ActorsAndActresses;

/**
 * Questa classe modella una coppia di valori (String, Integer) rappresentanti
 * l'attore ed il numero di film interpretati
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