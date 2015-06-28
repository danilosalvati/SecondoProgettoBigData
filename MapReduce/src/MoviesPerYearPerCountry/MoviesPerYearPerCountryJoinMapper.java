package MoviesPerYearPerCountry;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Il mapper costruisce le coppie (film, nazione) o (film, anno)
 * a seconda se sta parsando il file delle nazioni o
 * quello dei film e le invia al reducer che fa il join
 *
 */
public class MoviesPerYearPerCountryJoinMapper extends
Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		/* Per prima cosa divido l'input in maniera opportuna */


		String[] values = value.toString().split("\t");

		context.write(new Text(values[0].toString()), new Text(values[1].toString()));

	}
}