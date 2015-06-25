package ActorsAndActresses250TopMovies;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ActorsAndActresses250TopMoviesReducer extends
Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String result = "";

		for (Text value : values) {
			result = result + value.toString() + " - ";
		}
		
		try {
			result = result.substring(0, result.length() - 3);
		} catch (Exception e) {			
		}

		if (result.contains("<TOJOIN>")) {
			result = result.replace("<TOJOIN> - ", "");
			result = result.replace(" - <TOJOIN>", "");
			
			context.write(key, new Text(result));
		}

	}

}