package MostProlificYear;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Il mapper costruisce le coppie (anno, numeroFilm) e le invia al
 * reducer che seleziona l'anno con più film
 *
 */
public class MostProlificYearMapper extends
Mapper<Object, Text, Text, IntWritable> {

	private static final IntWritable one = new IntWritable(1);

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		/* Per prima cosa divido l'input in maniera opportuna */
		String[] values = value.toString().split("\t");

		if (values.length > 1) {
			/*
			 * Se ho almeno due elementi (nome film ed anno) calcolo il risultato,
			 * altrimenti escludo la riga
			 */
			
			String year = values[1];
			
			/* Scrivo la coppia (anno, numeroFilm) */
			context.write(new Text(year), one);
		}
	}
}