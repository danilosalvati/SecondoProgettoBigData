package MostProlificYear;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MostProlificYear {

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {

		if (args.length != 2) {
			System.err.println("USAGE: <hdfs movies path> <hdfs output path>");
			System.exit(1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MostProlificYear");

		job.setJarByClass(MostProlificYear.class);
		job.setMapperClass(MostProlificYearMapper.class);
		job.setCombinerClass(MostProlificYearReducer.class);

		job.setNumReduceTasks(1); // un solo reducer per evitare che ognuno calcoli il suo top
		job.setReducerClass(MostProlificYearReducer.class);

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