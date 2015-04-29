package cs5621.project2.job2;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public class KeyPartitioner extends Partitioner<Text, Text>{

	@Override
	public int getPartition(Text key, Text val, int numPartitions) {
		
		int index = 0;
		// Pull wayID from key
		index = ((Text) key).toString().indexOf('\t');
		Text keyWayID = new Text(((Text) key).toString().substring(0, index));
		
		// Partition by wayID
		return Integer.valueOf(keyWayID.toString()).hashCode()%numPartitions;
	}
}
