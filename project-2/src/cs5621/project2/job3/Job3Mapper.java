package cs5621.project2.job3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job3Mapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text newKey = new Text();
	private Text newValue = new Text();
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String part[] = value.toString().split("\t");
		String newPart[]=part[1].toString().split(" ");
		newKey = new Text("DeFaultKey"+"\t"+newPart[4]);
		newValue = new Text(part[0]+" "+part[1]);
		
		
		context.write(newKey, newValue);
	}
	
}

