package cs5621.project2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cs5621.project2.job1.Job1Mapper;
import cs5621.project2.job1.Job1Reducer;
import cs5621.project2.job2.CompositeKeyComparator;
import cs5621.project2.job2.Job2Mapper;
import cs5621.project2.job2.Job2Reducer;
import cs5621.project2.job2.KeyGroupingComparator;
import cs5621.project2.job2.KeyPartitioner;
import cs5621.project2.job3.Job3Mapper;
import cs5621.project2.job3.Job3Reducer;

/**
 * The Distance class configures each job with all required parameters. This
 * includes the command-line arguments, which are parsed and passed to the jobs.
 * Each job is run in order, taking into account the interdependencies between
 * them.
 * 
 * @author Brad Cutshall
 * @author Vamsidhar Kasireddy
 *
 */
public class Distance extends Configured implements Tool {

	/**
	 * Main method. Runs an instance of the project.
	 * 
	 * @param args
	 *            command-line arguments
	 * @throws Exception
	 *             if an error is encountered
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err
					.println("Enter valid number of arguments <Inputdirectory>  <Outputlocation> TOPN");
			System.exit(0);
		}
		ToolRunner.run(new Configuration(), new Distance(), args);
	}

	/**
	 * Runs an instance of the project.
	 */
	public int run(String[] args) throws Exception {
		final String OUTPUT_PATH = "intermediate_output";
		final String OUTPUT_PATH1 = "intermediate_output1";
		Configuration conf = new Configuration();

		// Configure the parameter
		conf.setInt("topN", Integer.parseInt(args[2]));

		// Job1

		Job job1 = new Job(conf, "distance");
		job1.setJarByClass(Distance.class);
		job1.setMapperClass(Job1Mapper.class);
		job1.setReducerClass(Job1Reducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setMapOutputKeyClass(LongWritable.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setNumReduceTasks(128);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH));

		job1.waitForCompletion(true);

		// Job2
		
		Job job2 = new Job(conf, "distance");
		job2.setJarByClass(Distance.class);
		job2.setPartitionerClass(KeyPartitioner.class);
		job2.setGroupingComparatorClass(KeyGroupingComparator.class);
		job2.setSortComparatorClass(CompositeKeyComparator.class);
		job2.setMapperClass(Job2Mapper.class);
		job2.setReducerClass(Job2Reducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setNumReduceTasks(128);
		FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
		FileOutputFormat.setOutputPath(job2, new Path(OUTPUT_PATH1));
		

		job2.waitForCompletion(true);

		// Job3

		Job job3 = new Job(conf, "topN");
		job3.setJarByClass(Distance.class);
		job3.setMapperClass(Job3Mapper.class);
		job3.setReducerClass(Job3Reducer.class);
		job3.setMapOutputKeyClass(NullWritable.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(NullWritable.class);
		job3.setOutputKeyClass(Text.class);
		job3.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job3, new Path(OUTPUT_PATH1));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]));


		return job3.waitForCompletion(true) ? 0 : 1;

	}

}
