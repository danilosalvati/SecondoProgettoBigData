package Directors250TopMovies;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Directors250TopMovies {

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {

		if (args.length != 2) {
			System.err
					.println("USAGE: <hdfs directors path> <hdfs output path>");
			System.exit(1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Directors250TopMovies");

		job.setJarByClass(Directors250TopMovies.class);
		job.setMapperClass(Directors250TopMoviesMapper.class);

		job.setNumReduceTasks(1);

		job.setReducerClass(Directors250TopMoviesReducer.class);
		job.setReducerClass(Directors250TopMoviesReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		try {
			FileInputFormat.addInputPath(job, new Path(args[0]));
		} catch (Exception e) {
			System.err.println("Error opening input path");
		}
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}