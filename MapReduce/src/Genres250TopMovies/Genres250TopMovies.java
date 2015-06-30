package Genres250TopMovies;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Genres250TopMovies {

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {

		if (args.length != 2) {
			System.err.println("USAGE: <hdfs genres path> <hdfs output path>");
			System.exit(1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Genres250TopMovies");

		job.setJarByClass(Genres250TopMovies.class);
		job.setMapperClass(Genres250TopMoviesMapper.class);

		job.setNumReduceTasks(1);

		job.setReducerClass(Genres250TopMoviesReducer.class);
		job.setReducerClass(Genres250TopMoviesReducer.class);

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