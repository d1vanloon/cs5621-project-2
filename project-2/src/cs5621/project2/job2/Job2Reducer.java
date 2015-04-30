package cs5621.project2.job2;

/**
 * @author Vamsidhar Kasireddy
 * @author Brad Cutshall
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

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException, NumberFormatException {
		
		Calculator c = new Calculator();
		double firstPoint[] = new double[2];// Stores the first node point
		double secondPoint[] = new double[2];// Stores the second node point
		double distance = 0;// Stores the distance between two nodes

		Text prevValue = null;
		int valueCount = 0;
		Text streetName = new Text();
		Text newValue = new Text();
		String firstPart[] = null;
		String prevPart[] = null;
		
		for (Text value : values) {

			streetName = new Text();
			newValue = new Text();
			
			
			if(valueCount > 0) {
				
				firstPart = value.toString().split(" ");// Splits the values on space
				int flag = Integer.parseInt(firstPart[4]);// variable to identify whether
														  // the node is a
														  // intersection
				
				prevPart = prevValue.toString().split(" ");
				
				if (flag != 1) {
					try {
						System.out.println("FirstPoint:" + firstPart[1] + " " + firstPart[2]);
						System.out.println("SecondPoint:" + prevPart[1] + " " + prevPart[2]);
						
						
						firstPoint[0] = Double.parseDouble(firstPart[1]);
						firstPoint[1] = Double.parseDouble(firstPart[2]);

						
						secondPoint[0] = Double.parseDouble(prevPart[1]);
						secondPoint[1] = Double.parseDouble(prevPart[2]);
						distance += c.latLongDistance(firstPoint, secondPoint);

						System.out.println("Distance Add:" + distance);
						prevValue = new Text(value.toString());
						
					} catch (NullPointerException e) {
						System.out.println("NullPointer:" + e.getStackTrace());
					}
					
				} else {

					try {
						firstPoint[0] = Double.parseDouble(firstPart[1]);
						firstPoint[1] = Double.parseDouble(firstPart[2]);
						secondPoint[0] = Double.parseDouble(prevPart[1]);
						secondPoint[1] = Double.parseDouble(prevPart[2]);
						distance += c.latLongDistance(firstPoint, secondPoint);
						
						if (firstPart.length == 6) {
							streetName = new Text(firstPart[5]);
						} else {
							streetName = new Text("No Way Name");
						}
						
						newValue = new Text(streetName + " " + firstPoint[0] + " " + firstPoint[1]);
						context.write(new Text(String.valueOf(distance)), newValue);

						distance = 0;
						valueCount = 0;
						
					} catch (NullPointerException e) {
						System.out.println("NullPointer:" + e.getStackTrace());
					}
				}
				
			} else {
				prevValue = new Text(value.toString());
				valueCount++;
				
			}
		}
		
		// Abnormal base case of reducer values size == 1 
		if (valueCount == 1) {
			prevPart = prevValue.toString().split(" ");
			
			if (prevPart.length == 6) {
				streetName = new Text(prevPart[5]);
			} else {
				streetName = new Text("No Way Name");
			}
			
			newValue = new Text(streetName + " " + prevPart[1] + " " + prevPart[2]);
			context.write(new Text(String.valueOf(distance)), newValue);
		}
	}
}