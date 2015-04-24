package cs5621.project2.job1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class Job1Test {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Job");

		job.setJarByClass(Job1Test.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Job1Mapper.class);

		job.setReducerClass(Job1Reducer.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(128);

		FileInputFormat.addInputPath(job, new Path(args[3]));

		FileOutputFormat.setOutputPath(job, new Path(args[3] + "/sampleOutput/"));

		job.waitForCompletion(true);
		
	}
	
	
}
