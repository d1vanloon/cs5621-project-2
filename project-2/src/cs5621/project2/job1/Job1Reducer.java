package cs5621.project2.job1;
import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Job1Reducer extends Reducer<LongWritable, Text, Text, Text> {
	 public void reduce(LongWritable key, Iterable<Text> values, 
			 Context context) throws IOException, InterruptedException {
		 //Strings to contain the value information that will be split based on blank spaces
		 //information will either be way information or a pair of coordinates
		 String [] coordinates = null;
		 String [] wayInfo = null;
		 for(Text value:values){
			 //if the first character of the string is a 'w' we have a way
			 if(value.charAt(0) == 'w'){
				 wayInfo = value.toString().split(" ");
			 }
			 //else we have a node
			 else{
				 coordinates = value.toString().split(" ");
			 }
			 // concatenate the coordinates to the wayInfo
			 wayInfo.toString().concat(" " + coordinates[0]);
			 wayInfo.toString().concat(" " + coordinates[1]);
			 //if it is an intersection append '1' to the way info
			 if(isIntersection(values)){
				 wayInfo.toString().concat(" " + '1');
			 }
			 //else it is not an intersection, append '0' to the way info
			 else{
				 wayInfo.toString().concat(" " + '0');
			 }
			 //newKey should contain WayID, Way Name, and the Node Index
			 //newValue should contain Node Latitude, Node Longitude,  Node Index, and Intersection Flag
			 Text newKey = new Text(wayInfo[0] + " " + wayInfo[3] + " " + wayInfo[2]);
			 Text newValue = new Text(wayInfo[4] + " " + wayInfo[5] + " " + wayInfo[2] + " " + wayInfo[6]);
			 
			 //Write the key values
			 context.write(newKey, newValue);
		 }
		 
		 
		
	 }//end reducer function

	 
	 /*utility function to determine if we have an intersection
	  * takes in a group of values
	  * returns true or false depending on whether or not
	  * there are more than 3 values in the group 
	  */
	 public boolean isIntersection(Iterable<Text> values){
		 //use a counter
		 int counter = 0;
		 //to count how many values (nodes) are in this reduce function
		 for(Text value : values){
			 counter++;
		 }
		 //if it's greater than 3, we have an intersection
		 if(counter >= 3) {
			 return true;
		 }
		 //else it's not an intersection
		 else
		 return false;
	 }
}//end reducer class
