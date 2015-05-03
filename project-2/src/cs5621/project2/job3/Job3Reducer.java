package cs5621.project2.job3;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job3Reducer extends
		Reducer<NullWritable, Text, NullWritable, Text> {

	@Override
	public void reduce(NullWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		TreeMap<Double, Text> topStretch = new TreeMap<Double, Text>();
		NavigableMap<Double, Text> descTopStretch = new TreeMap<Double,Text>();
		
		for (Text value : values) {
			int topN = new Integer(context.getConfiguration().get("topN"));
		
			String valPart[] = value.toString().split("\t");
			String parts[] = valPart[1].toString().split(" ");

			Double distance = Double.parseDouble(parts[4]);
			topStretch.put(new Double(distance),new Text( value));
			
			if (topStretch.size() > topN) {
				topStretch.remove(topStretch.firstKey());
			}

		}
descTopStretch = topStretch.descendingMap();
		for (Text val : descTopStretch.values()) {
			context.write(NullWritable.get(), val);
		}

	}

}