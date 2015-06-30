package Countries250TopMovies_old;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Il mapper costruisce le coppie (film, nazione) o (film, "<TOJOIN>")
 * a seconda se sta parsando il file delle nazioni o
 * quello dei 250 top film e le invia al reducer che fa il join
 *
 */
public class Countries250TopMoviesJoinMapper extends
Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		/* Per prima cosa divido l'input in maniera opportuna */

		if (value.toString().startsWith("0000")) {
			/* Se sto parsando il file dei film... */
			
			String[] values = value.toString().split("\t");

			try {
				context.write(new Text(values[3]), new Text("<TOJOIN>"));
			} catch (Exception e) {
				
			}
			
		} else {
			/* Se sto parsando il file delle nazioni... */
			
			String[] values = value.toString().split("\t");
			
			String title = values[0].toString().split("\\)")[0] + ")";
			title = title.replaceAll("\"","");

			if (values[1].toString().contains("<ENDVALUE>")) {
				String[] countries = values[1].toString().split("<ENDVALUE>");
				
				for (String country : countries) {
					/* Scrivo la coppia (film, nazione)*/
					
					context.write(new Text(title), new Text(country));
				}
			} else {
				context.write(new Text(title), new Text(values[1].toString()));
			}		

		}

	}
}