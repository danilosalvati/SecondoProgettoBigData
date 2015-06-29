package ActorsAndActresses250TopMovies;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Il mapper costruisce le coppie (attore, 1) se il film
 * è nei top 250 e le invia al reducer
 *
 */
public class ActorsAndActresses250TopMoviesMapper extends
Mapper<Object, Text, Text, IntWritable> {

	private static final IntWritable one = new IntWritable(1);

	private static ArrayList<String> top250movies = new ArrayList<String>();

	/*protected void setup(Context context)
			throws IOException,
			InterruptedException {

		if (top250movies.isEmpty()) {
						
			try {
				Path path = new Path("hdfs://input/top250movies.list"); // path del file in HDFS
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
				String line = br.readLine();

				while (line != null){
					String[] values = line.split("\t");

					top250movies.add(values[3]);
										
					line = br.readLine();
				}
				
			} catch(Exception e) {
				System.out.println("ERROR: File 'hdfs://input/top250movies.list' not found!");
			}
		}
		
		//top250movies.add("boh");	//se decommenti questa riga va ma tira fuori dati sbagliati

	}*/

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		// ho messo qua perchè nel setup non andava, ma qua ci mette una vita e non so se va
		if (top250movies.isEmpty()) {
			
			try {
				Path path = new Path("hdfs://input/top250movies.list"); // path del file in HDFS
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
				String line = br.readLine();

				while (line != null){
					String[] values = line.split("\t");

					top250movies.add(values[3]);
										
					line = br.readLine();
				}
				
			} catch(Exception e) {
				System.err.println("ERROR: File 'hdfs://input/top250movies.list' not found!");
			}
		}
		
		/* Per prima cosa divido l'input in maniera opportuna */

		String[] values = value.toString().split("\t");
		boolean isInTop250 = false;

		if (values[1].toString().contains("<ENDVALUE>")) {
			String[] movies = values[1].toString().split("<ENDVALUE>");

			for (String movie : movies) {
				/* Scrivo la coppia (attore, 1) per tutti i film dell'attore
				 * se il film è nei top 250*/

				/* Controllo se il film è nei top 250 */
				isInTop250 = false;

				for (String topMovie : top250movies) {
					if (movie.startsWith(topMovie)) {
						isInTop250 = true;
					}
				}

				if (isInTop250) {
					context.write(new Text(values[0].toString()), one);
				}
			}
		} else {
			/* Controllo se il film è nei top 250 */
			isInTop250 = false;

			for (String topMovie : top250movies) {
				if (values[1].toString().startsWith(topMovie)) {
					isInTop250 = true;
				}
			}

			if (isInTop250) {
				context.write(new Text(values[0].toString()), one);
			}
		}		
	}

	protected void cleanup(Context context)
			throws IOException,
			InterruptedException {

		top250movies.clear();

	}
}