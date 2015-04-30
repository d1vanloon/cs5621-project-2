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
		
		System.out.println("REDUCEREDUCE::" + key);
		
		Calculator c = new Calculator();
		double firstPoint[] = new double[2];// Stores the first node point
		double secondPoint[] = new double[2];// Stores the second node point
		double distance = 0;// Stores the distance between two nodes

		Text prevValue = null;
		int counter = 0;
		Text streetName = new Text();
		Text newValue = new Text();
		String firstPart[] = null;
		String prevPart[] = null;
		int flag = 0;
		
		for (Text value : values) {

			streetName = new Text();
			newValue = new Text();
			
			
			if(counter > 0) {
				
				prevPart = prevValue.toString().split(" ");
				firstPart = value.toString().split(" ");// Splits the values on space
				flag = Integer.parseInt(firstPart[4]);// variable to identify whether
														  // the node is a
														  // intersection
				
				
				
				try {
					String debug = new String();
					debug += ("FirstPoint:" + firstPart[1] + " " + firstPart[2]);
					debug += ("SecondPoint:" + prevPart[1] + " " + prevPart[2]);
					if (firstPart.length == 6) {
						streetName =  new Text(firstPart[5]);
					} else {
						streetName = new Text("No Way Name");
					}
					debug += streetName;
					//System.out.println("Counter:" + counter + " " + debug + " " + flag);
											
					firstPoint[0] = Double.parseDouble(firstPart[1]);
					firstPoint[1] = Double.parseDouble(firstPart[2]);
						
					secondPoint[0] = Double.parseDouble(prevPart[1]);
					secondPoint[1] = Double.parseDouble(prevPart[2]);
					distance += c.latLongDistance(firstPoint, secondPoint);
					
					//System.out.println("Distance Add:" + distance);
					//prevValue = new Text(value.toString());
						
				} catch (NullPointerException e) {
					System.out.println("NullPointer:" + e.getStackTrace());
				}
					
				if (flag == 1) {

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
						
						newValue = new Text(streetName + " " + firstPoint[0] + " " + firstPoint[1] + " " + key);
						context.write(new Text(String.valueOf(distance)), newValue);

						distance = 0;
						counter = 0;
						flag = 0;
						
					} catch (NullPointerException e) {
						System.out.println("NullPointer:" + e.getStackTrace());
					}
				}
				
			}
			prevValue = new Text(value.toString());
			counter++;
			
		}
		
		// Abnormal base case of reducer values size == 1 or end of road without intersection
		if (flag == 0 && distance > 0) {
			prevPart = prevValue.toString().split(" ");
			
			if (prevPart.length == 6) {
				streetName = new Text(prevPart[5]);
			} else {
				streetName = new Text("No Way Name");
			}
			
			newValue = new Text(streetName + " " + prevPart[1] + " " + prevPart[2] + " " + key);
			
			
			context.write(new Text(String.valueOf(distance)), newValue);
		}
	}
}