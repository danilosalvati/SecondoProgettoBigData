package MostProlificActorsAndActresses;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Il mapper costruisce le coppie (anno, numeroFilm) e le invia al reducer che
 * seleziona l'anno con più film
 *
 */
public class MostProlificActorsAndActressesMapper extends
		Mapper<Object, Text, Text, IntWritable> {

	private static final IntWritable one = new IntWritable(1);

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		/* Per prima cosa divido l'input in maniera opportuna */
		String[] values = value.toString().split("\t");
		String title = "";

		if (values[1].toString().contains("<ENDVALUE>")) {
			String[] movies = values[1].toString().split("<ENDVALUE>");

			for (String movie : movies) {
				/*
				 * Scrivo la coppia (attore, 1) per tutti i film
				 * dell'attore/attrice
				 */
				title = movie.split("\\)")[0] + ")";
				title = title.replaceAll("\"", "");
				context.write(new Text(values[0].toString()), one);
			}
		} else {
			title = values[1].toString().split("\\)")[0] + ")";
			title = title.replaceAll("\"", "");
			context.write(new Text(values[0].toString()), one);
		}

	}
}