package cs5621.project2.job3;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public class NaturalKeyPartitioner extends Partitioner<Text, Text>{

	@Override
	public int getPartition(Text key, Text val, int numPartitions) {
		
		int partition = 0;
		int hash = 0;
		
		Double distance = Math.ceil(Double.valueOf(key.toString()));

		partition = distance.intValue() % numPartitions;
		// Pull DefaultKey from key
		//index = ((Text) key).toString().indexOf("\t");
		// return number of partitions
		
		
		
		 return partition;
		//return (key.toString().substring(0, index).hashCode())%numPartitions;
		
	}
}

