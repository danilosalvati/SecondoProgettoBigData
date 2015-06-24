package ActorsAndActresses250TopMovies;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Il mapper costruisce le coppie (film, attore) o (film, "<TOJOIN>")
 * (a seconda se sta parsando il file degli/delle attori/attrici o
 * quello dei 250 top film e le invia al reducer che fa il join
 *
 */
public class ActorsAndActresses250TopMoviesMapper extends
Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		/* Per prima cosa divido l'input in maniera opportuna */

		if (value.toString().startsWith("0000")) {
			/* Se sto parsando il file dei film... */
			
			String[] values = value.toString().split("\t");

			context.write(new Text(values[3]), new Text("<TOJOIN>"));

		} else {
			/* Se sto parsando il file degli/delle attori/attrici... */
			
			String[] values = value.toString().split("\t");
			String title = "";

			if (values[1].toString().contains("<ENDVALUE>")) {
				String[] movies = values[1].toString().split("<ENDVALUE>");
				
				for (String movie : movies) {
					/* Scrivo la coppia (film, attore) per tutti i film dell'attore*/
					title = movie.split("\\)")[0] + ")";
					title = title.replaceAll("\"","");
					context.write(new Text(title), new Text(values[0].toString()));
				}
			} else {
				title = values[1].toString().split("\\)")[0] + ")";
				title = title.replaceAll("\"","");
				context.write(new Text(title), new Text(values[0].toString()));
			}		

		}

	}
}