/**
 * 
 */
package cs5621.project2.job1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Job 1 mapper class. The mapper function for Job 1 is tasked with reading the
 * flattened input data and grouping data by node ID.
 * 
 * @author David Van Loon
 *
 */
public class Job1Mapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
	 * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		super.map(key, value, context);
	}

}
