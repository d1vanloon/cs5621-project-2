package cs5621.project2.job2;

/**
 * @author Vamsidhar Kasireddy
 */
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs5621.project2.tools.Calculator;

/*
 * Calculates the distance between each intersection and sends its to the next MapReduce Job
 * Selects two Immediate neighbors in a way and finds the distance between those nodes
 * 
 */
public class Job2Reducer extends Reducer<Text, Text, Text, Text> {
	public Text newKey;
	public Text newValue;

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException, NumberFormatException {
		Calculator c = new Calculator();
		double point1[] = null;// Stores the first node point
		double point2[] = null;// Stores the second node point
		double interPoint[] = null;// Stores the intersection point
		double distance = 0;// Stores the distance between two nodes
		int count = 0;// counter to identify the first point in the way
		double newDistance = 0;// Stores the distance between one intersection
								// to the next
		for (Text value : values) {
			String part[] = value.toString().split(" ");// Splits the values on
														// space

			int flag = Integer.parseInt(part[4]);// variable to identify whether
													// the node is a
													// intersection
			if (flag != 1) {
				if (count == 0) {

					try {
						point1[0] = Double.parseDouble(part[1]);
						point1[1] = Double.parseDouble(part[2]);

						count++;
						interPoint = point1;
					} catch (NullPointerException e) {

					}
				}
				try {
					point2[0] = Double.parseDouble(part[1]);

					point2[1] = Double.parseDouble(part[2]);
					distance = c.latLongDistance(point1, point2);
					newDistance += distance;
					point1 = point2;
				} catch (NullPointerException e) {

				}

			} else {
				if (count == 0) {
					try {
						point1[0] = Double.parseDouble(part[1]);
						point1[1] = Double.parseDouble(part[2]);
						count++;
						interPoint = point1;
					} catch (NullPointerException e) {

					}
				}
				try {
					point2[0] = Double.parseDouble(part[1]);
					point2[1] = Double.parseDouble(part[2]);
					distance = c.latLongDistance(point1, point2);
					newDistance += distance;

					if (part.length == 6)
						newKey.set(part[5]);
					else
						newKey.set("No Way Name");
					newValue.set(interPoint + " " + point2 + " " + newDistance);
					context.write(newKey, newValue);
					point1 = point2;
					newDistance = 0;
					interPoint = point2;
				} catch (NullPointerException e) {

				}

			}

		}
	}

}