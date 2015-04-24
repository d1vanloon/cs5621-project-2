package cs5621.project2.job2;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job2Mapper extends Mapper<Text, Text, Text, Text> {
		
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		context.write(key, value);
	}
	
}
