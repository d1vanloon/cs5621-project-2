package cs5621.project2.job1;
import java.io.IOException;
import java.util.ArrayList;

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
		String isIntersection = "";
		//check to see if we have an intersection...
		if(isIntersection(values)){
			isIntersection = "1";
		 }
		 //else it is not an intersection, append '0' to the way info
		 else{
			 isIntersection = "0";
		 }
		
		 for(Text value:values){
			 //if the first character of the string is a 'w' we have a way
			 if(!value.toString().startsWith("way")){
				 coordinates = value.toString().split(" ");
				 break;
			 } 
		 }
		 
		 
		for(Text value:values){
			if(value.charAt(0) == 'w'){
				Text newKey;
				wayInfo = value.toString().split(" ");
						 
				if(wayInfo.length > 3){
			       newKey = new Text(wayInfo[1] + " " + wayInfo [3] + " " + wayInfo[2]);
				}
				else{
					  newKey = new Text(wayInfo[1] + " " + wayInfo[2]);
				}
					 								//lat             lon 			        index				intersection
				 Text newValue = new Text(coordinates[0] + " " + coordinates[1] + " " + wayInfo[2] + " " + isIntersection);
		 
				//Write the key values
				  context.write(newKey, newValue);
	     		}//end if
		 	}//end for
	     
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
