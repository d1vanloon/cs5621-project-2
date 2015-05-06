package cs5621.project2.job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * @author Brad Cutshall
 *
 * Using the a comparator is necessary since we need to perform a 
 * complex transaction between the mappers and reducers in job 2.
 * A key during job 2 will be used to reduce way segments by their
 * id, and also provide sorted input into the reducer.
 */
public class CompositeKeyComparator extends WritableComparator {

	public CompositeKeyComparator() {
		super(Text.class, true);

	}

	/**
	 * Comparator function to compare to incoming keys.
	 * Output: -1, 0, 1 for -1 for less than, 1 for greater and 0 for equal.
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable k1, WritableComparable k2)
			throws IndexOutOfBoundsException {
		
		// Setup the comparator
		int index = 0;
		Text k1NodeIndex = new Text();
		Text k2NodeIndex = new Text();
		IntWritable k1NodeIndexNum = new IntWritable();
		IntWritable k2NodeIndexNum = new IntWritable();

		// Pull nodeIndex from k1
		index = ((Text) k1).toString().indexOf(" ");

		try {
			k1NodeIndex = new Text(((Text) k1).toString().substring(index + 1));

			try {
				k1NodeIndexNum = new IntWritable(Integer.parseInt(k1NodeIndex
						.toString()));

			} catch (NumberFormatException e) {
				System.out.println("first CompositeKeyComparator Error:"
						+ e.getMessage());
			}
		} catch (IndexOutOfBoundsException e) {
			System.out.println("second CompositeKeyComparator Error:"
					+ e.getMessage());
		}

		// Pull nodeID from k2
		index = ((Text) k2).toString().indexOf(" ");

		try {
			k2NodeIndex = new Text(((Text) k2).toString().substring(index + 1));
			try {
				k2NodeIndexNum = new IntWritable(Integer.parseInt(k2NodeIndex
						.toString()));
			} catch (NumberFormatException e) {
				System.out.println("third CompositeKeyComparator Error:"
						+ e.getMessage());
			}
		} catch (IndexOutOfBoundsException e) {
			System.out.println("fourth CompositeKeyComparator Error:"
					+ e.getMessage());
		}
		

			Text k1WayID = new Text(((Text) k1).toString().substring(0, index));

			Text k2WayID = new Text(((Text) k2).toString().substring(0, index));
			int compare = k1WayID.compareTo(k2WayID);

		

		// Return a inequality answer.
		// compareTo:: return (thisValue<thatValue ? -1 : (thisValue==thatValue
		// ? 0 : 1));
		// Returns 1 if k2 < k1 = -1, else 1
		if (compare == 0)
			return k1NodeIndexNum.compareTo(k2NodeIndexNum);

		return compare;

	}

}