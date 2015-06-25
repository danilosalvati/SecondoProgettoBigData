package ActorsAndActresses250TopMovies;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ActorsAndActresses250TopMovies {

	public static void main(String[] args) throws ClassNotFoundException,
	IOException, InterruptedException {

		if (args.length != 4) {
			System.err.println("USAGE: <hdfs actors path> <hdfs actresses path> <hdfs top250movies path> <hdfs output path>");
			System.exit(1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ActorsAndActresses250TopMovies");

		job.setJarByClass(ActorsAndActresses250TopMovies.class);
		job.setMapperClass(ActorsAndActresses250TopMoviesMapper.class);

		//job.setNumReduceTasks(1);
		job.setReducerClass(ActorsAndActresses250TopMoviesReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		try {
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileInputFormat.addInputPath(job, new Path(args[1]));
			FileInputFormat.addInputPath(job, new Path(args[2]));
		} catch (Exception e) {
			System.err.println("Error opening input path");
		}
		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}