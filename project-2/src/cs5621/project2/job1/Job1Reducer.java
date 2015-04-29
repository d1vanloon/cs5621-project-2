package cs5621.project2.job1;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job1Reducer extends Reducer<LongWritable, Text, Text, Text> {
	/**
	 * reduce function takes in the output from the Job1Mapper class
	 * @param key is the node ID 
	 * @param value is a line of text consisting of one of two possible values
	 * The value will either be a set of coordinates (Latitude , Longitude)
	 * OR it will be a line of text in the format "way wayID node index and the name of the way"
	 * 
	 * This function iterates through the list of values corresponding to the Node ID and formats them
	 * in a manner that is appropriate for the second job
	 * 
	 * OUTPUT KEY = way ID , Node index
	 * OUTPUT VALUE = Node ID , Latitude , Longitude , Node index , Intersection Flag , Way Name 
	 */

	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		//ArrayList to store a copy of all of the values getting passed into this reduce functions
		ArrayList<Text> valuesCopy = new ArrayList<Text>();
		//Splitable string that will contain the coordinate values
		String[] coordinates = null;
		//Splitable string that will hold the way information values
		String[] wayInfo = null;
		//intersection flag
		String isIntersection = "";
		//Convert the node ID which is the key, to a string to be used for output
		String nodeID = key.toString();
		
		//copy the values into the ArrayList
		for (Text value : values) {
			valuesCopy.add(new Text(value));
		}
		// check to see if we have an intersection. If there is one, set the Intersection flag to "1" 
		if (isIntersection(valuesCopy)) {
			isIntersection = "1";
		}
		// else it is not an intersection, set the Intersection flag to "0"
		else {
			isIntersection = "0";
		}
		//For the values in the ArrayList, extract the coordinates.
		//Note: there will be only one set of coordinates per unique key
		for(Text value : valuesCopy){
			//if the value doesn't start with "way" we have a coordinate set
			if (!value.toString().startsWith("way")) {
				//split the value by blank space, and store it in the coordinates variable
				coordinates = value.toString().split(" ");
				//break out of the loop
				break;
			}
			
		}
		//For the other values in the ArrayList
		for (Text value : valuesCopy) {
			//If we have a way
			if (value.charAt(0) == 'w') {
				Text newKey;
				Text newValue;
				//Split the value by blank space, and store it in the wayInfo variable
				wayInfo = value.toString().split(" ");
				//If the way has a length greater than 3, it has a way name
				//set our new output value to:
				//node ID , Lat , Long , index , Intersection , way Name
				if (wayInfo.length > 3) {
					newValue = new Text(nodeID + " " + coordinates[0] + " " + coordinates[1]
							+ " " + wayInfo[2] + " " + isIntersection + " "
							+ wayInfo[3]);
				} 
				//else create the value in the same format as above, without the way name. 
				else {
					newValue = new Text(nodeID + " " + coordinates[0] + " " + coordinates[1]
							+ " " + wayInfo[2] + " " + isIntersection);

				}
				//Make a new key consisting of the way ID, and the node Index
				newKey = new Text(wayInfo[1] + " " + wayInfo[2]);

			    // Write the key values
				context.write(newKey, newValue);
			}// end if
		}// end for

	}// end reducer function

	/**
	 *Utility function to determine whether or not we are dealing with an intersection
	 *@param valuesCopy, is an ArrayList containing all of the Text values that are passed
	 *into this reducer. 
	 *
	 *If there are 3 or more values in the ArrayList, it is an intersection
	 *@return true or false depending on whether or not it is an intersection. 
	 */
	public boolean isIntersection(ArrayList<Text> valuesCopy) {

		if (valuesCopy.size() >= 3) {
			return true;
		}
		else
			return false;
	}
}// end reducer class
