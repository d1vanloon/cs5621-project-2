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
		
		double firstPoint[] = new double[2];// Stores the first point of way segement
		double newPoint[] = new double[2];// Stores the new node point
		double prevPoint[] = new double[2];// Stores the second node point
		double distance = 0;// Stores the distance between two nodes
		
		Text streetName = new Text();
		Text newValue = new Text();
		Text prevValue = null;
		
		String firstPart[] = null;
		String newPart[] = null;
		String prevPart[] = null;
		
		int counter = 0;
		int flag = 0;
		
		for (Text value : values) {

			// Reset new key and value 
			streetName = new Text();
			newValue = new Text();
			
			// If there is a previous value, we can compute distance and check intersection
			if(counter > 0) {
				
				// Splits the values on space
				prevPart = prevValue.toString().split(" ");
				newPart = value.toString().split(" ");
				
				// Add new points to total segment distance
				try {
					newPoint[0] = Double.parseDouble(newPart[1]);
					newPoint[1] = Double.parseDouble(newPart[2]);
						
					prevPoint[0] = Double.parseDouble(prevPart[1]);
					prevPoint[1] = Double.parseDouble(prevPart[2]);
					distance += c.latLongDistance(newPoint, prevPoint);
					
					// variable to identify whether the node is an intersection
					flag = Integer.parseInt(newPart[4]);
						
				} catch (NullPointerException e) {
					System.out.println("NullPointer:" + e.getStackTrace());
				}
				// It is an intersection so add new item and print
				if (flag == 1) {

					try {
						
						if (newPart.length == 6) {
							streetName = new Text(newPart[5]);
						} else {
							streetName = new Text("No Way Name");
						}
						
						newValue = new Text(firstPoint[0] + " " + firstPoint[1] + " " + newPoint[0] + " " + newPoint[1] + " " + distance);
						context.write(streetName, newValue);

						//Reset for new segment
						distance = 0;
						counter = 0;
						flag = 0;
						
					} catch (NullPointerException e) {
						System.out.println("NullPointer:" + e.getStackTrace());
					}
				}
				
			}
			
			
			// Parse and save the first element of segment
			if (counter == 0) {
				try {
					firstPart = value.toString().split(" ");
					firstPoint[0] = Double.parseDouble(firstPart[1]);
					firstPoint[1] = Double.parseDouble(firstPart[2]);
				} catch (NullPointerException e) {
					System.out.println("NullPointer:" + e.getStackTrace());
				}
				
			}
			
			// Assign current value to previous for next loop
			prevValue = new Text(value.toString());
			counter++;
			
		}
		
		// Abnormal case of end of road without intersection
		if (distance > 0) {
			//newPart = prevValue.toString().split(" ");
			
			
			if (prevPart.length == 6) {
				streetName = new Text(prevPart[5]);
			} else {
				streetName = new Text("No Way Name");
			}
			
			newValue = new Text(firstPoint[0] + " " + firstPoint[1] + " " + newPoint[0] + " " + newPoint[1] + " " + distance);
			context.write(streetName, newValue);
		}
	}
}