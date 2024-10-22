package cs5621.project2.job2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Text;

/**
 * 
 * @author Brad Cutshall
 * 
 * Since a composite key is used in Job 2, this comparator is
 * used to ensure that reducers will get values that are
 * associated with the same way segment.
 *
 */
public class KeyGroupingComparator extends WritableComparator{

	public KeyGroupingComparator() {
		super(Text.class, true);
		
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable k1, WritableComparable k2) throws IndexOutOfBoundsException {
		
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
