package cs5621.project2.job2;
 
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

public class CompositeKeyComparator extends WritableComparator {
	
	public CompositeKeyComparator() {
		super(Text.class, true);
		
	}
	 
	@Override
	public int compare(WritableComparable k1, WritableComparable k2) {
		int index = 0;
		
		// Pull wayID and nodeID from k1
		index = ((Text) k1).toString().indexOf('\t');
		Text k1NodeID = new Text(((Text) k1).toString().substring(index));
		IntWritable k1WayIDNum = new IntWritable(Integer.valueOf(k1NodeID.toString()));
		
		// Pull wayID and nodeID from k2 
		index = ((Text) k2).toString().indexOf('\t');
		Text k2NodeID = new Text(((Text) k2).toString().substring(index));
		IntWritable k2WayIDNum = new IntWritable(Integer.valueOf(k2NodeID.toString()));
		
		// compareTo:: return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
		// Returns 1 if k2 < k1 = -1, else 1
		return  k2WayIDNum.compareTo(k1WayIDNum);
		
	}

}