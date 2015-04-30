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

	@SuppressWarnings("null")
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException, NumberFormatException {
		Text newKey = null;
		Text newValue = null;
		double point1[] = { 0, 0 };// Stores the first node point
		double point2[] = { 0, 0 };// Stores the second node point
		double interPoint[] = { 0, 0 };// Stores the intersection point
		// Stores the distance between two nodes
		int count = 0;// counter to identify the first point in the way
		double newDistance = 0;
		double distance;// Stores the distance between one intersection
						// to the next
		for (Text value : values) {

			String part[] = value.toString().split(" ");// Splits the values on
			// System.out.println("part0 "+part[0]+"part1 "+part[1]+"part2 "+part[2]+"part3 "+part[3]+"part4 "+part[4]);
			// // space

			int flag = Integer.parseInt(part[4]);// variable to identify whether
													// the node is a
													// intersection
			if (flag != 1) {
				if (count == 0) {

					try {
						point1[0] = Double.parseDouble(part[1].trim());
						point1[1] = Double.parseDouble(part[2].trim());
						// System.out.println("first point"+point1[0]);
						count++;
						interPoint = point1;
					} catch (NullPointerException e) {
						System.out.println("first exception");
					}
				}
				if (count != 0) {
					try {
						point2[0] = Double.parseDouble(new String(part[1]));

						point2[1] = Double.parseDouble(new String(part[2]));
						distance = new Calculator().latLongDistance(point1,
								point2);
						System.out.println(distance);
						newDistance += distance;
						// System.out.println("new distance"+newDistance);
						point1 = point2;
					} catch (NullPointerException e) {
						System.out.println("Second exception");

					}
				}

			} else {

				if (count == 0) {
					try {
						point1[0] = Double.parseDouble(new String(part[1]));
						point1[1] = Double.parseDouble(new String(part[2]));
						count++;
						interPoint = point1;
					} catch (NullPointerException e) {
						System.out.println("third exception");
					}
				}

				if (count != 0) {
					try {

						point2[0] = Double.parseDouble(new String(part[1]));

						point2[1] = Double.parseDouble(new String(part[2]));
						distance = new Calculator().latLongDistance(point1,
								point2);
						newDistance += distance;
						// System.out.println("new distance 1 "+newDistance);
						if (part.length == 6) {
							newKey = new Text(part[5]);
						} else
							newKey = new Text("No Way Name");

					} catch (NullPointerException e) {
						System.out.println("last exception" + e.getMessage());
					}

					newValue = new Text(interPoint[0] + " " + interPoint[1]
							+ " " + point2[0] + " " + point2[1] + " "
							+ newDistance);

					context.write(newKey, newValue);
					point1 = point2;
					newDistance = 0;
					interPoint = point2;

				}

			}

		}
	}

}