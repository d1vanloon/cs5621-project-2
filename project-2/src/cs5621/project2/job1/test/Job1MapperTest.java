/**
 * 
 */
package cs5621.project2.job1.test;

import static org.junit.Assert.*;

import org.junit.Test;

import cs5621.project2.job1.Job1Mapper;

/**
 * Test cases for Job 1 Mapper.
 * 
 * @author David Van Loon
 *
 */
public class Job1MapperTest {
	
	String nodeInputLine = "<node id=\"19193992\" version=\"2\" "
			+ "timestamp=\"2012-03-30T16:43:58Z\" uid=\"10786\" "
			+ "user=\"stucki1\" changeset=\"11157125\" "
			+ "lat=\"46.846285\" lon=\"-91.24682\"/>";
	String nodeInputLineLongitude = "-91.24682";
	String nodeInputLineLatitude = "46.846285";
	
	/**
	 * Test method for {@link cs5621.project2.job1.Job1Mapper#map(org.apache.hadoop.io.LongWritable, org.apache.hadoop.io.Text, org.apache.hadoop.mapreduce.Mapper.Context)}.
	 */
	@Test
	public void testMapLongWritableTextContext() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link cs5621.project2.job1.Job1Mapper#extractLongitude(java.lang.String)}.
	 */
	@Test
	public void testExtractLongitude() {
		Job1Mapper map1 = new Job1Mapper();
		String foundLongitude = map1.extractLongitude(nodeInputLine);
		assertTrue(foundLongitude.equals(nodeInputLineLongitude));
	}

	/**
	 * Test method for {@link cs5621.project2.job1.Job1Mapper#extractUniqueAttribute(java.lang.String, java.lang.String)}.
	 */
	@Test
	public void testExtractUniqueAttribute() {
		Job1Mapper map1 = new Job1Mapper();
		String inputLine = "the attribute is here=\"value here\" and the rest of the line";
		String foundValue = map1.extractUniqueAttribute(inputLine, "here");
		assertTrue(foundValue.equals("value here"));
	}

	/**
	 * Test method for {@link cs5621.project2.job1.Job1Mapper#extractLatitude(java.lang.String)}.
	 */
	@Test
	public void testExtractLatitude() {
		Job1Mapper map1 = new Job1Mapper();
		String foundLatitude = map1.extractLatitude(nodeInputLine);
		assertTrue(foundLatitude.equals(nodeInputLineLatitude));
	}

	/**
	 * Test method for {@link cs5621.project2.job1.Job1Mapper#extractNodeIDFromNodeLine(java.lang.String)}.
	 */
	@Test
	public void testExtractNodeIDFromNodeLine() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link cs5621.project2.job1.Job1Mapper#extractNodeIDFromWayLine(java.lang.String)}.
	 */
	@Test
	public void testExtractNodeIDFromWayLine() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link cs5621.project2.job1.Job1Mapper#extractWayID(java.lang.String)}.
	 */
	@Test
	public void testExtractWayID() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link cs5621.project2.job1.Job1Mapper#extractNodeIndex(java.lang.String)}.
	 */
	@Test
	public void testExtractNodeIndex() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link cs5621.project2.job1.Job1Mapper#extractRoadName(java.lang.String)}.
	 */
	@Test
	public void testExtractRoadName() {
		fail("Not yet implemented");
	}

}
