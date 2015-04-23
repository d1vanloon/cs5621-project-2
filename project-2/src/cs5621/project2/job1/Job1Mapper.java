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
	
	/**
	 * Enumerated type defining a counter group and counters
	 * for categorizing each input line.
	 * @author David Van Loon
	 *
	 */
	public static enum InputLineTypes {
		NODE_DETAILS,
		WAY_ENTRY,
		NOT_USEFUL
	}

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
		
		/*
		 * Input: 
		 * 
		 * Interesting data is in the following format(s):
		 * 
		 * <node id="19193953" version="2" timestamp="2012-03-30T16:43:55Z" uid="10786" user="stucki1" changeset="11157125" lat="46.858986" lon="-91.234218"/>
		 * <way id="18248027" version="4" timestamp="2015-03-31T04:46:02Z" uid="97431" user="Dion Dock" changeset="29867642"> index=5 <nd ref="188137163"/> <tag k="name" v="Plum Street"/> <tag k="highway" v="residential"/>
		 * 
		 * Generally:
		 * <node id="{node id}" version="2" timestamp="2012-03-30T16:43:55Z" uid="10786" user="stucki1" changeset="11157125" lat="{latitude}" lon="{longitude}"/>
		 * <way id="{way id}" version="4" timestamp="2015-03-31T04:46:02Z" uid="97431" user="Dion Dock" changeset="29867642"> index={node index} <nd ref="{node id}"/> <tag k="name" v="{road name}"/> <tag k="highway" v="residential"/>
		 * 
		 * Uninteresting data that is similar:
		 * </node>
		 * <relation id="116017" version="159" timestamp="2015-01-07T17:20:27Z" uid="1376118" user="ChrisZontine" changeset="27980825">
		 * <member type="way" ref="186409174" role="forward"/>
		 * </relation>
		 * 
		 * For lines beginning with "<node":
		 *  - The <node id> is the value between "id=\"" and the next "\""
		 *  - The <latitude> is the value between "lat=\"" and the next "\""
		 *  - The <longitude> is the value between "lon=\"" and the next "\""
		 *  
		 * For lines beginning with "<way":
		 *  - The <node id> is the value between "ref=\"" and the next "\""
		 *  - The <way id> is the value between "way id=\"" and the next "\""
		 *  - The <node index> is the integer value directly following "index="
		 *  - The <road name> is the value between "v=\"" and the next "\""
		 * 
		 * Output: 
		 * 
		 * Key: <node id> (as LongWritable)
		 * Value: "<latitude> <longitude>" OR "way <way id> <node index> [road~name]"
		 * 
		 * Where names in <> are replaced with values, names in [] are 
		 * optionally replaced with values, and values are separated 
		 * by spaces. The road name will have spaces replaced with '~'.
		 */
		
		// Get the line of input data
		String line = value.toString();
		
		// Decide whether we're processing node details or a way entry
		if (line.startsWith("<node")) {
			// We're processing a line with node details
			
			// Increment the number of node details lines processed
			context.getCounter(InputLineTypes.NODE_DETAILS).increment(1);
		} else if (line.startsWith("<way")) {
			// We're processing a line with a way entry
			
			// Increment the number of way entry lines processed
			context.getCounter(InputLineTypes.WAY_ENTRY).increment(1);
		} else {
			// We've got a line that is not useful - count it and disregard
			context.getCounter(InputLineTypes.NOT_USEFUL).increment(1);
		}
		
		super.map(key, value, context);
	}

}
