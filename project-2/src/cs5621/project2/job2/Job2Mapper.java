package cs5621.project2.job2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job2Mapper extends Mapper<Text, Text, Text, Text> {
	private final static Text newValue = new Text();
	private Text newKey = new Text();
	
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		String part[] = value.toString().split("\t");
		newKey.set(part[0]);
		newValue.set(part[1]);
		
		
		
		context.write(newKey,newValue);
	}
	
}
