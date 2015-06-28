package MoviesPerYearPerCountry;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Il mapper costruisce le coppie (nazioneAnno, 1)
 * e le invia al reducer che fa la somma
 *
 */
public class MoviesPerYearPerCountryCountMapper extends
Mapper<Object, Text, Text, IntWritable> {

	private static final IntWritable one = new IntWritable(1);

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		/* Per prima cosa divido l'input in maniera opportuna */
		String[] values = value.toString().split("\t");
		String[] countriesAndYear = values[1].toString().split("<ENDVALUE>");
		
		String year = "";
		ArrayList<String> countries = new ArrayList<String>();
		
		for (String countryOrYear : countriesAndYear) {
			if (countryOrYear.contains("1") || countryOrYear.contains("2") 
					|| countryOrYear.contains("?") || countryOrYear.contains("-")) {
				year = countryOrYear;
			} else {
				countries.add(countryOrYear);
			}
		}
		
		for (String country : countries) {
			context.write(new Text(country + " " + year), one);
		}
		
	}
}