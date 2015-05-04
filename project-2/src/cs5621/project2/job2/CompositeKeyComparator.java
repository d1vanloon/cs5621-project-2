package cs5621.project2.job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {

	public CompositeKeyComparator() {
		super(Text.class, true);

	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable k1, WritableComparable k2)
			throws IndexOutOfBoundsException {
		int index = 0;
		int compare = -1;
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
		try {

			Text k1WayID = new Text(((Text) k1).toString().substring(0, index));

			Text k2WayID = new Text(((Text) k2).toString().substring(0, index));
			compare = k1WayID.compareTo(k2WayID);

		} catch (IndexOutOfBoundsException e) {
			System.out.println("Exception" + ((Text) k1).toString() + " "
					+ ((Text) k2).toString());
		}

		// compareTo:: return (thisValue<thatValue ? -1 : (thisValue==thatValue
		// ? 0 : 1));
		// Returns 1 if k2 < k1 = -1, else 1
		if (compare == 0)
			return k1NodeIndexNum.compareTo(k2NodeIndexNum);

		return compare;

	}

}