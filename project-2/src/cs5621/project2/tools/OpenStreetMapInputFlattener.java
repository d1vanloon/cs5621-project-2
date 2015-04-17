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
	 */
	public void flatten() {
		for (int x = 0; x < 2; x++) {
			try {
				currentLine = reader.readLine();
				System.out.println("Read: " + currentLine);
				out.write(currentLine.getBytes());
				System.out.println("Output.");
				out.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
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
			new OpenStreetMapInputFlattener(inputFile, outputFile).flatten();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
