package cs5621.project2.job3;
/*
 * 
 * author: Vamsidhar Kasireddy 
 * 
 * Functionality : 
 */
import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job3Mapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	TreeMap<Double, Text> topStretch = new TreeMap<Double, Text>();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// Obtain the user parameter which governs how many top N
		// roads will be included.
		int topN = new Integer(context.getConfiguration().get("topN"));
		
		// Remove the street name from the incoming value to separate
		// the rest of the data.
		String valPart[] = value.toString().split("\t");
		
		// We will be sorting by distance, so we remove the distance
		// from the value string and use this value to sort.
		String parts[] = valPart[1].toString().split(" ");
		Double distance = Double.parseDouble(parts[4]);

		
		topStretch.put(new Double(distance), new Text(value));
		
		if (topStretch.size() > topN) {
		
			topStretch.remove(topStretch.firstKey());
			
		}

	}
	
	protected void cleanup(Context context) throws IOException,InterruptedException {
		
		for (Text road : topStretch.values())
		{
			context.write(NullWritable.get(), road);
		}
		
	}

}
