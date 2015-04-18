/**
 * 
 */
package cs5621.project2.tools;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.LinkedList;

/**
 * Flattens OpenStreetMap data into a format more easily interpreted in a
 * line-by-line manner.
 * 
 * @author David Van Loon
 *
 */
public class OpenStreetMapInputFlattener {

	private static final String K_NAME = "k=\"name\"";
	private static final String K_HIGHWAY = "k=\"highway\"";
	private static final String ND = "<nd";
	private static final String TAG = "<tag";
	private static final String WAY_END = "</way";
	private static final String WAY = "<way";
	File inputFile;
	File outputFile;
	BufferedReader reader;
	OutputStream out;
	String currentLine;
	LinkedList<String> currentWayNodes;
	String currentWayAttributes;
	String currentWayTags;

	/**
	 * Initializes a new OpenStreetMapInputFlattener
	 * 
	 * @param inputFile
	 *            the input file to read
	 * @throws IOException
	 */
	public OpenStreetMapInputFlattener(File inputFile, File outputFile)
			throws IOException {
		this.inputFile = inputFile;
		this.outputFile = outputFile;

		this.reader = new BufferedReader(new InputStreamReader(
				Files.newInputStream(inputFile.toPath())));
		this.out = new BufferedOutputStream(Files.newOutputStream(outputFile
				.toPath()));

		this.currentWayNodes = new LinkedList<String>();
		this.currentLine = "";
		this.currentWayAttributes = "";
		this.currentWayTags = "";
	}

	/**
	 * Flattens the input file into the output file.
	 * 
	 * This process removes all ways that are not roads. Ways that represent
	 * roads are stripped so that the outer way tags are removed and each node
	 * line is prepended with the opening way tag and appended with the name
	 * and/or highway tags.
	 * 
	 * @return the number of lines processed
	 */
	public int flatten() {
		int processedLines = 0;
		currentLine = "";
		// Track if we're processing a way
		boolean inWay = false;
		// Track if the current way is a road
		boolean isRoad = false;
		String trimmedLine;

		try {
			while ((currentLine = reader.readLine()) != null) {
				trimmedLine = currentLine.trim();
				// If we're processing the body of a way
				if (inWay) {
					// If the current line is a node
					if (trimmedLine.startsWith(ND)) {
						// Store the current node
						currentWayNodes.add(trimmedLine);
					} else
					// If the current line is a highway or name tag
					if ((trimmedLine.startsWith(TAG))
							&& (trimmedLine.contains(K_HIGHWAY) || trimmedLine
									.contains(K_NAME))) {
						// Append the tag to the current way tags
						currentWayTags += " " + trimmedLine;
						// Mark if the current way is a road
						isRoad = trimmedLine.contains(K_HIGHWAY);
					} else
					// If the current line is the end of the way
					if (trimmedLine.startsWith(WAY_END)) {
						// For each node
						while (!currentWayNodes.isEmpty()) {
							// Get the node and remove it from the list
							String node = currentWayNodes.removeFirst();
							// Output the node with the way data in front and
							// the way tags
							// behind
							// Only if the current way is a road
							if (isRoad) {
								out.write((currentWayAttributes + " " + node + currentWayTags)
										.getBytes());
								// ... and a new line
								out.write(System.lineSeparator().getBytes());
							}
						}
						// We're done with the way
						inWay = false;
						currentWayTags = "";
					}
				} else
				// If we're currently at the start of a way
				if (trimmedLine.startsWith(WAY)) {
					// Set the current way attributes to the newly discovered
					// way
					currentWayAttributes = trimmedLine;
					// We're now processing a way
					inWay = true;
					// Assume it isn't a road until told otherwise
					isRoad = false;
				} else
				// If we're looking at any other line
				{
					// Just output the line
					out.write(trimmedLine.getBytes());
					// ... and a new line
					out.write(System.lineSeparator().getBytes());
				}
				processedLines++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		// Flush the output buffer
		try {
			out.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return processedLines;
	}

	/**
	 * Runs the OpenStreetMapInputFlattener
	 * 
	 * @param args
	 *            the command-line arguments
	 */
	public static void main(String[] args) {
		File inputFile = new File(args[0]);
		File outputFile = new File(args[1]);
		try {
			System.out.println("Processed lines: "
					+ new OpenStreetMapInputFlattener(inputFile, outputFile)
							.flatten());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
