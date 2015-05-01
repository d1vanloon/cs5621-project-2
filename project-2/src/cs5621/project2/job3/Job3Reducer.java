package cs5621.project2.job3;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.io.Text;

public class Job3Reducer extends Reducer<Text, Text, Text, Text> {
	

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		
		System.out.println("New Reducer");
		Text newKey = new Text();
		Text newValue = new Text();
		LinkedList<Text> list = new LinkedList<Text>();

		int topN = 1; // Default to one
		try {
			topN = new Integer(context.getConfiguration().get("topN"));
		} catch (NumberFormatException e) {
			System.out.println("Job3Reducer: " + e.getStackTrace());
		}

		int count = 0;
		int counter = 0;
		for (Text value : values) {
			counter = counter+1;
			
			/*
			for (Text item : list) {
				Double 
				Double listItem = Double.valueOf(item.toString());
				
				if ()
			}
			*/
			if (count < topN) {
				count = count + 1;
				String part[] = value.toString().split(" ");
				// String keyPart[] = key.toString().split("\t");
				newKey = new Text(part[4]);
				newValue = new Text(part[0] + " " + part[1] + " " + part[2]
						+ " " + part[3] + " " + key.toString());
				context.write(new Text("Road, Largest: " + count + " out of " + topN), new Text());
				context.write(newKey, newValue);
			}
		}
	}

}