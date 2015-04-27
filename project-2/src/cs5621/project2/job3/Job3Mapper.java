package cs5621.project2.job3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job3Mapper extends Mapper<Text, Text, Text, Text> {
	private Text newKey = new Text();
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		String part[] = value.toString().split("\t");
		newKey.set("DeFaultKey"+"\t"+part[3]);
		
		context.write(newKey, value);
	}
	
}
