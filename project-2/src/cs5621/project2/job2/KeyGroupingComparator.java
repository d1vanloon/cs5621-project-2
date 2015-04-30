package cs5621.project2.job2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Text;

public class KeyGroupingComparator extends WritableComparator{

	public KeyGroupingComparator() {
		super(Text.class, true);
		
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable k1, WritableComparable k2) {
		
		int index = 0;
		// Pull wayID and nodeID from k1
		index = ((Text) k1).toString().indexOf(" ");
		Text k1WayID = new Text(((Text) k1).toString().substring(0, index));
		
		// Pull wayID and nodeID from k2 
		index = ((Text) k2).toString().indexOf(" ");
		Text k2WayID = new Text(((Text) k2).toString().substring(0, index));
	
		// Returns 0 if equal
		return k1WayID.compareTo(k2WayID);
	}
}
