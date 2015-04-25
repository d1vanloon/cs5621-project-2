package cs5621.project2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;

import cs5621.project2.job1.*;
import cs5621.project2.job2.*;
import cs5621.project2.job3.*;

public class Distance {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		// Job1 
		/*
		Job job1 = Job.getInstance(conf, "distance");
		job1.setJarByClass(Distance.class);
		job1.setMapperClass(Job1Mapper.class);
		job1.setReducerClass(Job1Reducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		
		job1.waitForCompletion(true);
		*/
		
		// Job2 
		Job job2 = Job.getInstance(conf, "distance");
		job2.setJarByClass(Distance.class);
		job2.setMapperClass(Job2Mapper.class);
		job2.setReducerClass(Job2Reducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		job2.waitForCompletion(true);
		
		
		// Job3 
		/*
		Job job3 = Job.getInstance(conf, "distance");
		job3.setJarByClass(Distance.class);
		job3.setMapperClass(Job3Mapper.class);
		job3.setReducerClass(Job3Reducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputKeyClass(Text.class);
		
		FileInputFormat.addInputPath(job3, new Path(args[0]));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]));
		
		job3.waitForCompletion(true);
		*/
		
	}
	
}