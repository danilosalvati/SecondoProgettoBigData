package QuotesNumberFor250TopMovies;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Il mapper costruisce le coppie (film, 1)
 * e le invia al reducer che fa la somma
 *
 */
public class QuotesNumberFor250TopMoviesCountMapper extends
Mapper<Object, Text, Text, IntWritable> {

	private static final IntWritable one = new IntWritable(1);

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		/* Per prima cosa divido l'input in maniera opportuna */
		String[] values = value.toString().split("\t");
		String[] quotes = values[1].toString().split("<ENDVALUE>");
		
		for (String quote : quotes) {
			context.write(new Text(values[0].toString()), one);
		}
		
	}
}