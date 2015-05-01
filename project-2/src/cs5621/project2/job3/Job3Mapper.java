package cs5621.project2.job3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job3Mapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	
		Text newKey = new Text();
		Text newValue = new Text();
		
		String valPart[] = value.toString().split("\t");
		String wayName = valPart[0].toString();
		String parts[] = valPart[1].toString().split(" ");

		newKey = new Text(parts[4]);
		newValue = new Text(parts[0]+" "+parts[1] + " " + parts[2] + " " + parts[3] + " " + wayName);
		
		
		context.write(newKey, newValue);
	}
	
}

