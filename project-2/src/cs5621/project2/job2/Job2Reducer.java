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
 * Calculates the distance of a way segment between each intersection.
 * This reducer's will output the way segment's way name, two points
 * referring to the beginning and end intersections, and the computed
 * way segment distance. We are only concerned about the way name, the 
 * way's Latitude and Longitude value pairs of the way segment's start 
 * and end coordinates, and the distance we can ignore the incoming so
 * we can ignore the Key.  The values must come into the reducer sorted
 * by the order they exist in the way segment.
 * 
 * Input: 
 * 	Key: <Text Way ID> 
 * 		The key of this job is the segment's way ID acts only to reduce
 * 		all of a way segment's nodes to a single reducer.
 * 	Values: <[Node ID], Latitude, Longitude, [Order in Way Segment], Intersection Flag {0,1}, Way Name>
 * 		Latitude, Longitude coordinate points of 
 * 
 * Output:
 * 	Key: <Way Name>
 * 	Value: <Start Lat, Start Lon, End Lat, End Lon, Segment Distance>
 * 
 */
public class Job2Reducer extends Reducer<Text, Text, Text, Text> {

	/**
	 * Reducer for Job2, calculates the distance of each way segment between intersections.
	 */
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException, NumberFormatException {
		
		Calculator c = new Calculator();
		
		double firstPoint[] = new double[2];// Stores the first point of way segment
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
			
		} // End value loop (for)
		
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
	// Reducer Complete
}