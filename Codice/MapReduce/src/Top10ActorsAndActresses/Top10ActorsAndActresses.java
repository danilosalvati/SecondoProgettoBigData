package Top10ActorsAndActresses;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top10ActorsAndActresses {

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {

		if (args.length != 3) {
			System.err
					.println("USAGE: <hdfs actors path> <hdfs actresses path> <hdfs output path>");
			System.exit(1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Top10ActorsAndActresses");

		job.setJarByClass(Top10ActorsAndActresses.class);
		job.setMapperClass(Top10ActorsAndActressesMapper.class);
		job.setCombinerClass(Top10ActorsAndActressesReducer.class);

		job.setNumReduceTasks(1); // un solo reducer per evitare che ognuno
									// calcoli la sua top 10
		job.setReducerClass(Top10ActorsAndActressesReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		try {
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileInputFormat.addInputPath(job, new Path(args[1]));
		} catch (Exception e) {
			System.err.println("Error opening input path");
		}
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}