package cs5621.project2.job3;


import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.io.Text;

public class Job3Reducer extends Reducer<Text, Text, Text, Text> {
	public Text newKey = null;
	public Text newValue = null;
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		org.apache.hadoop.conf.Configuration conf3 = context.getConfiguration();
		int topN = Integer.parseInt(conf3.get("topN"));
		int count =0;
		for(Text value : values) {
			if(count<topN)
			{
				String part[] = value.toString().split("\t");
				newKey.set(part[0]);
				newValue.set(part[1]+"\t"+part[2]+"\t"+part[3]);
						
			context.write(newKey, newValue);
			}
		}
	}
	
}