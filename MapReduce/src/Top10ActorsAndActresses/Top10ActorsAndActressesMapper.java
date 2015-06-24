package Top10ActorsAndActresses;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Il mapper costruisce le coppie (attore, numeroFilm) e le invia al
 * reducer che seleziona i 10 attori con più film
 *
 */
public class Top10ActorsAndActressesMapper extends
Mapper<Object, Text, Text, IntWritable> {

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		/* Per prima cosa divido l'input in maniera opportuna */
		String[] values = value.toString().split("\t");

		if (values.length > 1) {
			/*
			 * Se ho almeno due elementi (nome attore e lista film) calcolo il risultato,
			 * altrimenti escludo la riga
			 */

			String[] movies = values[1].toString().split("<ENDVALUE>");
			
			/* Scrivo la coppia (attore, numeroFilm) */
			context.write(new Text(values[0].toUpperCase()), new IntWritable(movies.length));
		}
	}
}