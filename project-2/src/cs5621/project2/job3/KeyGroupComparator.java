package cs5621.project2.job3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyGroupComparator extends WritableComparator {

	public KeyGroupComparator() {
		super(Text.class, true);

	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable k1, WritableComparable k2) {

		int index = 0;
		// Pull the Default Key
		index = ((Text) k1).toString().indexOf("\t");
		Text k1Default = new Text(((Text) k1).toString().substring(0, index));

		// Pull the Default key
		index = ((Text) k2).toString().indexOf("\t");
		Text k2Default = new Text(((Text) k2).toString().substring(0, index));

		// Compare the keys and Return the Result
		return k2Default.compareTo(k1Default);
	}
}
