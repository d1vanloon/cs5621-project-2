package cs5621.project2.job2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job2Mapper extends Mapper<LongWritable, Text, Text, Text> {
	private final static Text newValue = new Text();
	private Text newKey = new Text();
	
	/**
	 * Mapper operation for the second job, where we calculate distances between intersections.
	 * Key: < WayID, Way Name, Node Index >
	 * Value: < lat, lon, node ID, node index, and intersection flag >
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		
		String part[] = value.toString().split("\t");
		//String addKey[] = part[0].split(" ");
		//System.out.println(addKey[1]);
		newKey.set(part[0]);
		newValue.set(part[1]);
		System.out.println("new Key"+newKey);
		System.out.println("newValue"+newValue);
		
		
		
		context.write(newKey,newValue);
	}
	
}
