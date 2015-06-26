package MoviesPerYearPerCountry;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Il mapper costruisce le coppie (nazioneAnno, 1)
 * e le invia al reducer che fa le somme
 *
 */
public class MoviesPerYearPerCountryMapper extends
Mapper<Object, Text, Text, IntWritable> {

	private static final IntWritable one = new IntWritable(1);

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		/* Per prima cosa divido l'input in maniera opportuna */
		String[] values = value.toString().split("\t");

		String title = values[0].toString().split("\\)")[0] + ")";
		title = title.replaceAll("\"","");

		try {
			context.write(new Text(values[1].toString() + " (" + title.split("\\(")[1]), one);
		} catch (Exception e) {

		}

	}
}