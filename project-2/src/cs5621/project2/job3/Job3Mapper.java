package cs5621.project2.job3;

/*
 * 
 * author: Vamsidhar Kasireddy 
 * 
 * This mapper reads in output lines of second reducer as value and splits them on tab and space 
 * Each mapper with the help of treemap will have its local topN distance values which are then
 * send to reducer using clean up function 
 * 
 * Input : 
 * Value :<Start Lat, Start Lon, End Lat, End Lon, Segment Distance>
 * 
 * Output : 
 * Key : Null
 * Value : <Start Lat, Start Lon, End Lat, End Lon, Segment Distance>
 * 
 */
import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job3Mapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	/*
	 * Tree map to store Stretch distance as key , road name, coordinates of
	 * stretch and stretch distance as values in ascending order
	 */
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
		// retrieves the distance and stores it
		Double distance = Double.parseDouble(parts[4]);

		// inserts key values pairs into the treemap

		topStretch.put(new Double(distance), new Text(value));
		// compares the map size with topN and if it increases , removes the key
		// with lowest distance value
		if (topStretch.size() > topN) {

			topStretch.remove(topStretch.firstKey());

		}

	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// writes the values of above tree map
		for (Text road : topStretch.values()) {
			context.write(NullWritable.get(), road);
		}

	}

}
