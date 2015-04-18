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

	File inputFile;
	File outputFile;
	BufferedReader reader;
	OutputStream out;
	String currentLine;
	LinkedList<String> currentWay;

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
	}

	/**
	 * Flattens the input file into the output file.
	 * 
	 * @return the number of lines processed
	 */
	public int flatten() {
		int processedLines = 0;
		currentLine = "";

		// Stream through the file until a way is encountered
		while (!currentLine.startsWith("<way")) {
			try {
				// Read a line
				currentLine = reader.readLine();
				// Exit the loop if there are no more lines remaining
				if (currentLine == null) {
					break;
				}
				// ... and immediately write it to the output file
				out.write(currentLine.getBytes());
				// With a new line for good measure
				out.write(System.lineSeparator().getBytes());
				out.flush();
				// Increment the number of processed lines
				processedLines++;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
