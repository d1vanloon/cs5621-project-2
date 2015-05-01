package cs5621.project2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;

import cs5621.project2.job1.*;
import cs5621.project2.job2.*;
import cs5621.project2.job3.*;

public class Distance {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length!=3)
		{
			System.err.println("Usage: Distance <in> <out> <N Roads>");
			System.exit(2);
		}
		//Set the command line argument value for M top languages to "WikiSpikes_M"
		conf.setInt("topN",Integer.parseInt(otherArgs[2]));
		
		// Job1
		/*
		 * Job job1 = Job.getInstance(conf, "sort");
		 * job1.setJarByClass(Distance.class);
		 * job1.setMapperClass(Job1Mapper.class);
		 * job1.setReducerClass(Job1Reducer.class);
		 * job1.setOutputKeyClass(Text.class);
		 * job1.setOutputKeyClass(Text.class);
		 * 
		 * FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		   FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]+"/FirstJobOutput"));
		 * 
		 * job1.waitForCompletion(true);
		 */
/*
		// Job2
		Job job2 = new Job(conf, "distance");
		job2.setJarByClass(Job2Test.class);
		job2.setPartitionerClass(KeyPartitioner.class);
		job2.setGroupingComparatorClass(KeyGroupingComparator.class);
		job2.setSortComparatorClass(CompositeKeyComparator.class);
		job2.setMapperClass(Job2Mapper.class);
		job2.setReducerClass(Job2Reducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputKeyClass(Text.class);

		//FileInputFormat.addInputPath(job2, new Path(otherArgs[1]+"/FirstJobOutput"));
		//FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]+"/SecondJobOutput"));
		FileInputFormat.addInputPath(job2, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));

		job2.waitForCompletion(true);
*/
		// Job3

		//conf.set("topN", args[2]); // set top N Stretches
		Job job3 = new Job(conf, "topN");
		job3.setJarByClass(Distance.class);
		job3.setPartitionerClass(NaturalKeyPartitioner.class);
		job3.setGroupingComparatorClass(KeyGroupComparator.class);
		job3.setSortComparatorClass(KeyComparator.class);
		job3.setMapperClass(Job3Mapper.class);
		job3.setReducerClass(Job3Reducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputKeyClass(Text.class);
		job3.setNumReduceTasks(Integer.parseInt(otherArgs[2]));

		//FileInputFormat.addInputPath(job3, new Path(otherArgs[1]+"/SecondJobOutput"));
		//FileOutputFormat.setOutputPath(job3, new Path(otherArgs[1]));
		FileInputFormat.addInputPath(job3, new Path(args[0]));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]));

		job3.waitForCompletion(true);


	}

}
