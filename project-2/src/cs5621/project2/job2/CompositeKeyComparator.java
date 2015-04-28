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
		Text k1NodeIndex = new Text();
		Text k2NodeIndex = new Text();
		IntWritable k1NodeIndexNum = new IntWritable();
		IntWritable k2NodeIndexNum = new IntWritable();
		
		
		// Pull nodeIndex from k1
		index = ((Text) k1).toString().indexOf('\t');
		
		try {
			k1NodeIndex = new Text(((Text) k1).toString().substring(index));
			try {
				k1NodeIndexNum = new IntWritable(Integer.valueOf(k1NodeIndex.toString()));
			} catch (NumberFormatException e) {
				System.out.println("CompositeKeyComparator Error: " + e.getMessage());
			}
		} catch (IndexOutOfBoundsException e) {
			System.out.println("CompositeKeyComparator Error: " + e.getMessage());
		}
		
		// Pull nodeID from k2 
		index = ((Text) k2).toString().indexOf('\t');
		
		try {
			k2NodeIndex = new Text(((Text) k2).toString().substring(index));
			try {
				k2NodeIndexNum = new IntWritable(Integer.valueOf(k2NodeIndex.toString()));
			} catch (NumberFormatException e) {
				System.out.println("CompositeKeyComparator Error: " + e.getMessage());
			}
		} catch (IndexOutOfBoundsException e) {
			System.out.println("CompositeKeyComparator Error: " + e.getMessage());
		}
		
		// compareTo:: return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
		// Returns 1 if k2 < k1 = -1, else 1
		return  k2NodeIndexNum.compareTo(k1NodeIndexNum);
		
	}

}