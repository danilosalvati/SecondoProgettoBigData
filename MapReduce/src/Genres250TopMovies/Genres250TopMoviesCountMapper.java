package Genres250TopMovies;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Il mapper costruisce le coppie (genere, 1)
 * e le invia al reducer che fa la somma
 *
 */
public class Genres250TopMoviesCountMapper extends
Mapper<Object, Text, Text, IntWritable> {

	private static final IntWritable one = new IntWritable(1);

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		/* Per prima cosa divido l'input in maniera opportuna */
		String[] values = value.toString().split("\t");
		String[] genres = values[1].toString().split("<ENDVALUE>");
		
		for (String genre : genres) {
			context.write(new Text(genre), one);
		}
		
	}
}