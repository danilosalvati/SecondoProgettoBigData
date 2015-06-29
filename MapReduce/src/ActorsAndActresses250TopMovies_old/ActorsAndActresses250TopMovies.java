package ActorsAndActresses250TopMovies_old;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ActorsAndActresses250TopMovies {

	private static final String OUTPUT_PATH = "/intermediate_output_actorsAndActresses";

	public static void main(String[] args) throws ClassNotFoundException,
	IOException, InterruptedException {

		if (args.length != 4) {
			System.err.println("USAGE: <hdfs actors path> <hdfs actresses path> <hdfs top250movies path> <hdfs output path>");
			System.exit(1);
		}

		/* Primo Job: esegue il join tra i tre file in input */
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ActorsAndActresses250TopMovies_join");

		job.setJarByClass(ActorsAndActresses250TopMovies.class);
		job.setMapperClass(ActorsAndActresses250TopMoviesJoinMapper.class);

		job.setReducerClass(ActorsAndActresses250TopMoviesJoinReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		try {
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileInputFormat.addInputPath(job, new Path(args[1]));
			FileInputFormat.addInputPath(job, new Path(args[2]));
		} catch (Exception e) {
			System.err.println("Error opening input path");
		}
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

		job.waitForCompletion(true);
		
		
		
		/* Secondo Job: conta i film per ciascun attore */
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "ActorsAndActresses250TopMovies_count");

		job2.setJarByClass(ActorsAndActresses250TopMovies.class);
		job2.setMapperClass(ActorsAndActresses250TopMoviesCountMapper.class);

		job2.setNumReduceTasks(1);
		job2.setReducerClass(ActorsAndActresses250TopMoviesCountReducer.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		try {
			FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
		} catch (Exception e) {
			System.err.println("Error opening input path");
		}
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}