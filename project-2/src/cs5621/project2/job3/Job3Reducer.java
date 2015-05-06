package cs5621.project2.job3;

/* author: Vamsidhar Kasireddy 
 * Reads in the output sent from the mapper with null value as the key and Road name,coordinates of two ends of stretch, distance 
 * of the stretch as value sorted in ascending order then finds the topN from the finalists sent by the mappers
 * 
 * 
 * Input : 
 * Key : Null
 * Value : <Start Lat, Start Lon, End Lat, End Lon, Segment Distance>	 
 * 
 * 
 * 
 * Output:
 *  Key : Null
 * Value : <Start Lat, Start Lon, End Lat, End Lon, Segment Distance>	 
 *
 *
 */
import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job3Reducer extends
		Reducer<NullWritable, Text, NullWritable, Text> {

	@Override
	public void reduce(NullWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		/*
		 * Tree map to store Stretch distance as key , road name, coordinates of
		 * stretch and stretch distance as values in ascending order
		 */
		TreeMap<Double, Text> topStretch = new TreeMap<Double, Text>();
		/*
		 * Navigable map to store the key values pairs of above tree map in
		 * descending order
		 */
		NavigableMap<Double, Text> descTopStretch = new TreeMap<Double, Text>();
		// stores the values in to the topStretch tree map in ascending order
		for (Text value : values) {
			int topN = new Integer(context.getConfiguration().get("topN"));// Stores
																			// the
																			// TOPN
																			// value
			String valPart[] = value.toString().split("\t");// Splits the values
															// on tab space
			String parts[] = valPart[1].toString().split(" ");// splits the
																// second part
																// of above
																// divided value
																// on space

			Double distance = Double.parseDouble(parts[4]);// stores distance of
															// stretch into
															// distance variable
			topStretch.put(new Double(distance), new Text(value));// inputs the
																	// key as
																	// distance
																	// and value
																	// as the
																	// key value
																	// pairs of
																	// topStretch
																	// Tree map

			if (topStretch.size() > topN) {// validates that the size of the
											// tree map does not go above TOPN
				topStretch.remove(topStretch.firstKey());// removes the lowest
															// distance value
															// once the size
															// increases more
															// than TOPN
			}

		}
		descTopStretch = topStretch.descendingMap();// Reverses the key order of
													// topStretch Tree map
		for (Text val : descTopStretch.values()) {// Iterates in the loop till
													// the last value
			context.write(NullWritable.get(), val);// Writes null value as key
													// and road
		}

	}
// Reducer complete
}