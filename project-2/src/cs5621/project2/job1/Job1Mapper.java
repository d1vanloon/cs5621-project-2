/**
 * 
 */
package cs5621.project2.job1;

import java.io.IOException;
import java.util.InputMismatchException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Job 1 mapper class. The mapper function for Job 1 is tasked with reading the
 * flattened input data and grouping data by node ID.
 * 
 * @author David Van Loon
 *
 */
public class Job1Mapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	
	/**
	 * Enumerated type defining a counter group and counters
	 * for categorizing each input line.
	 * @author David Van Loon
	 *
	 */
	public static enum InputLineTypes {
		NODE_DETAILS,
		WAY_ENTRY,
		NOT_USEFUL
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
	 * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		/*
		 * Input: 
		 * 
		 * Interesting data is in the following format(s):
		 * 
		 * <node id="19193953" version="2" timestamp="2012-03-30T16:43:55Z" uid="10786" user="stucki1" changeset="11157125" lat="46.858986" lon="-91.234218"/>
		 * <way id="18248027" version="4" timestamp="2015-03-31T04:46:02Z" uid="97431" user="Dion Dock" changeset="29867642"> index=5 <nd ref="188137163"/> <tag k="name" v="Plum Street"/> <tag k="highway" v="residential"/>
		 * 
		 * Generally:
		 * <node id="{node id}" version="2" timestamp="2012-03-30T16:43:55Z" uid="10786" user="stucki1" changeset="11157125" lat="{latitude}" lon="{longitude}"/>
		 * <way id="{way id}" version="4" timestamp="2015-03-31T04:46:02Z" uid="97431" user="Dion Dock" changeset="29867642"> index={node index} <nd ref="{node id}"/> <tag k="name" v="{road name}"/> <tag k="highway" v="residential"/>
		 * 
		 * Uninteresting data that is similar:
		 * </node>
		 * <relation id="116017" version="159" timestamp="2015-01-07T17:20:27Z" uid="1376118" user="ChrisZontine" changeset="27980825">
		 * <member type="way" ref="186409174" role="forward"/>
		 * </relation>
		 * 
		 * For lines beginning with "<node":
		 *  - The <node id> is the value between "id=\"" and the next "\""
		 *  - The <latitude> is the value between "lat=\"" and the next "\""
		 *  - The <longitude> is the value between "lon=\"" and the next "\""
		 *  
		 * For lines beginning with "<way":
		 *  - The <node id> is the value between "ref=\"" and the next "\""
		 *  - The <way id> is the value between "way id=\"" and the next "\""
		 *  - The <node index> is the integer value directly following "index="
		 *  - The <road name> is the value between "v=\"" and the next "\""
		 * 
		 * Output: 
		 * 
		 * Key: <node id> (as LongWritable)
		 * Value: "<latitude> <longitude>" OR "way <way id> <node index> [road~name]"
		 * 
		 * Where names in <> are replaced with values, names in [] are 
		 * optionally replaced with values, and values are separated 
		 * by spaces. The road name will have spaces replaced with '~'.
		 */
		
		// Get the line of input data
		String line = value.toString();

		// Decide whether we're processing node details or a way entry
		if (line.startsWith("<node")) {
			// We're processing a line with node details

			// Extract the node ID
			long nodeID = extractNodeIDFromNodeLine(line);

			// Extract the latitude
			String latitude = extractLatitude(line);

			// Extract the longitude
			String longitude = extractLongitude(line);

			// Format the output value as "<latitude> <longitude>"
			String outputValue = String
					.format("%1$s %2$s", latitude, longitude);

			// Write the key and value to the reducer
			context.write(new LongWritable(nodeID), new Text(outputValue));

			// Increment the number of node details lines processed
			context.getCounter(InputLineTypes.NODE_DETAILS).increment(1);
		} else if (line.startsWith("<way")) {
			// We're processing a line with a way entry

			// Extract the node ID
			long nodeID = extractNodeIDFromWayLine(line);

			// Extract the way ID
			String wayID = extractWayID(line);

			// Extract the node index
			int nodeIndex = extractNodeIndex(line);
			
			// Extract the road name
			String roadName = extractRoadName(line);
			
			// Format the output value as
			// "way <way id> <node index> [road~name]"
			String outputValue = (roadName == null ? String.format(
					"way %1$s $2$d", wayID, nodeIndex) : String.format(
					"way %1$s $2$d %3$s", wayID, nodeIndex, roadName));
			
			// Write the key and value to the reducer
			context.write(new LongWritable(nodeID), new Text(outputValue));

			// Increment the number of way entry lines processed
			context.getCounter(InputLineTypes.WAY_ENTRY).increment(1);
		} else {
			// We've got a line that is not useful - count it and disregard
			context.getCounter(InputLineTypes.NOT_USEFUL).increment(1);
		}
	}

	/**
	 * Extracts the longitude from an input line.
	 * 
	 * @param line
	 *            the input line
	 * @return the longitude
	 */
	public String extractLongitude(String line) {
		String attributeName = "lon";
		return extractUniqueAttribute(line, attributeName);
	}

	/**
	 * Extracts the value of an attribute from a line of input data.
	 * 
	 * @param line
	 *            the input line
	 * @param attributeName
	 *            the attribute name, e.g. "lat" or "lon"
	 * @return the value of the attribute
	 * @throws InputMismatchException
	 *             if the attribute is not found in the input line
	 */
	public String extractUniqueAttribute(String line, String attributeName)
			throws InputMismatchException {
		// Record the size of the attribute name
		int sizeOfAttributeName = attributeName.length();
		// The index at which the attribute starts
		int indexOfAttrStart = line.indexOf(attributeName + "=\"")
				+ sizeOfAttributeName + 2;
		if (indexOfAttrStart == -1) {
			throw new InputMismatchException("Attribute \"" + attributeName
					+ "\" does not exist in the input line.");
		}
		// The rest of the line
		String subStringAfterLonStart = line.substring(line
				.indexOf(attributeName + "=\"") + sizeOfAttributeName + 2);
		// The index at which the attribute ends
		int indexOfLonEnd = indexOfAttrStart
				+ subStringAfterLonStart.indexOf("\"");
		// Extract the attribute from this substring
		return line.substring(indexOfAttrStart, indexOfLonEnd);
	}

	/**
	 * Extracts the latitude from an input line.
	 * 
	 * @param line
	 *            the input line
	 * @return the latitude
	 */
	public String extractLatitude(String line) {
		String attributeName = "lat";
		return extractUniqueAttribute(line, attributeName);
	}

	/**
	 * Extracts the node ID from an node details input line.
	 * 
	 * @param line
	 *            the input line
	 * @return the node ID
	 * @throws NumberFormatException if there's an error parsing the data
	 */
	public long extractNodeIDFromNodeLine(String line)
			throws NumberFormatException {
		String attributeName = "id";
		return Long.parseLong(extractUniqueAttribute(line, attributeName));
	}

	/**
	 * Extracts the node ID from an way entry input line.
	 * 
	 * @param line
	 *            the input line
	 * @return the node ID
	 * @throws NumberFormatException if there's an error parsing the data
	 */
	public long extractNodeIDFromWayLine(String line)
			throws NumberFormatException {
		String attributeName = "ref";
		return Long.parseLong(extractUniqueAttribute(line, attributeName));
	}

	/**
	 * Extracts the way ID from an input line.
	 * 
	 * @param line
	 *            the input line
	 * @return the way ID
	 */
	public String extractWayID(String line) {
		String attributeName = "way id";
		return extractUniqueAttribute(line, attributeName);
	}

	/**
	 * Extracts the node index from an input line.
	 * 
	 * @param line
	 *            the input line
	 * @return the node index
	 * @throws NumberFormatException if there's an error parsing the data
	 */
	public int extractNodeIndex(String line) throws NumberFormatException {
		String attributeName = "index=";
		// Get the string starting at the end of the attribute name until the
		// end of the line
		String restOfLine = line.substring(line.indexOf(attributeName)
				+ attributeName.length());
		// Get the index of the next space
		int indexOfNextSpace = restOfLine.indexOf(" ");
		// Return the integer value
		return Integer.parseInt(restOfLine.substring(0, indexOfNextSpace));
	}
	
	/**
	 * Extracts the road name from an input line.
	 * 
	 * @param line
	 *            the input line
	 * @return the road name, or null if it does not exist in the input line
	 */
	public String extractRoadName(String line) {
		String attributeName = "v";
		try {
			// Get the raw road name
			String rawName = extractUniqueAttribute(line, attributeName);
			// Return the road name with '~' in place of spaces
			return rawName.replace(' ', '~');
		} catch (InputMismatchException ex) {
			// The road name was not included in the input line
			return null;
		}
	}
	
}
