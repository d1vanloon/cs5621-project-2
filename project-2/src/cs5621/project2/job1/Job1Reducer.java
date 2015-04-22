public class Job1Reducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	 public void reduce(Longwritable key, iterable<Text> values, 
			 Context context) throws IOException, InterruptedException {
		 
		 for(Text value:Values){
			 String [] tag = value.toString.split(",");
			 
		 }
		 
		 
	 }//end reducer function

}//end reducer class
