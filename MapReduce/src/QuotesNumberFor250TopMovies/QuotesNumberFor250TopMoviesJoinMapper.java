package QuotesNumberFor250TopMovies;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Il mapper costruisce le coppie (film, citazione) o (film, "<TOJOIN>")
 * a seconda se sta parsando il file delle citazioni o
 * quello dei 250 top film e le invia al reducer che fa il join
 *
 */
public class QuotesNumberFor250TopMoviesJoinMapper extends
Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		/* Per prima cosa divido l'input in maniera opportuna */

		if (value.toString().startsWith("0000")) {
			/* Se sto parsando il file dei film... */
			
			String[] values = value.toString().split("\t");

			context.write(new Text(values[3]), new Text("<TOJOIN>"));

		} else {
			/* Se sto parsando il file delle citazioni... */
			
			String[] values = value.toString().split("\t");
			
			String title = values[0].toString().split("\\)")[0] + ")";
			title = title.replaceAll("\"","");

			if (values[1].toString().contains("<ENDVALUE>")) {
				String[] quotes = values[1].toString().split("<ENDVALUE>");
				
				for (String quote : quotes) {
					/* Scrivo la coppia (film, citazione) per tutte le citazioni del film*/
					
					context.write(new Text(title), new Text(quote.toString()));
				}
			} else {
				context.write(new Text(title), new Text(values[1].toString()));
			}		

		}

	}
}