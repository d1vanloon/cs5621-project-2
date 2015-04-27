package cs5621.project2.job3;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

public class KeyComparator extends WritableComparator {
	
	public KeyComparator() {
		super(Text.class, true);
		
	}
	 
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable k1, WritableComparable k2) {
		int index = 0;
		
		// Pull distance from k1
		index = ((Text) k1).toString().indexOf("\t");
		Text k1Way = new Text(((Text) k1).toString().substring(index));
		IntWritable k1Distance = new IntWritable(Integer.valueOf(k1Way.toString()));
		
		// Pull distance from k2 
		index = ((Text) k2).toString().indexOf("\t");
		Text k2Way = new Text(((Text) k2).toString().substring(index));
		IntWritable k2Distance = new IntWritable(Integer.valueOf(k2Way.toString()));
		
		// compareTo:: return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
		// Returns 1 if k2 < k1 = -1, else 1
		return  k2Distance.compareTo(k1Distance);
		
	}

}