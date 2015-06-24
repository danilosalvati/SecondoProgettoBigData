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
			result = result + value.toString();
		}
		
		//context.write(key, new Text(result));

		if (result.contains("<TOJOIN>")) {
			context.write(key, new Text(result));
		}
		
	}

}