package it.uniroma3.bigDataProject;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

public class ArrayContainsSubstring extends FilterFunc {

	@Override
	public Boolean exec(Tuple input) throws IOException {
		/*
		 * Il primo argomento dell'input è la stringa rapppresentante il titolo
		 */
		String title = (String) input.get(0);

		/* Il secondo argomento è la tupla contenente tutti i film */
		Tuple filmArray = (Tuple) input.get(1);

		/*
		 * Controllo se uno dei film della filmografia dell'attore comincia con
		 * il titolo dato
		 */
		for (Object filmObject : filmArray) {
			String film = (String) filmObject;
			if (film.startsWith(title)) {
				return true;
			}
		}
		return false;
	}
}
