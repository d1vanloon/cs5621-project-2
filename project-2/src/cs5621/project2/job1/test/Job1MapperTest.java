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
	long nodeInputLineNodeID = 19193992;
	
	String wayInputLine = "<way id=\"21468157\" version=\"3\" "
			+ "timestamp=\"2013-04-21T11:05:06Z\" uid=\"451693\" "
			+ "user=\"bot-mode\" changeset=\"15808823\"> index=3 "
			+ "<nd ref=\"230917750\"/> <tag k=\"name\" "
			+ "v=\"South Hawkins Avenue\"/> "
			+ "<tag k=\"highway\" v=\"proposed\"/>";
	String wayInputLineWayID = "21468157";
	long wayInputLineNodeID = 230917750;
	String wayInputLineWayName = "South~Hawkins~Avenue";
	int wayInputLineNodeIndex = 3;
	String wayInputLineDescriptor = "proposed";

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
		Job1Mapper map1 = new Job1Mapper();
		long foundID = map1.extractNodeIDFromNodeLine(nodeInputLine);
		assertTrue(foundID == nodeInputLineNodeID);
	}

	/**
	 * Test method for {@link cs5621.project2.job1.Job1Mapper#extractNodeIDFromWayLine(java.lang.String)}.
	 */
	@Test
	public void testExtractNodeIDFromWayLine() {
		Job1Mapper map1 = new Job1Mapper();
		long foundID = map1.extractNodeIDFromWayLine(wayInputLine);
		assertTrue(foundID == wayInputLineNodeID);
	}

	/**
	 * Test method for {@link cs5621.project2.job1.Job1Mapper#extractWayID(java.lang.String)}.
	 */
	@Test
	public void testExtractWayID() {
		Job1Mapper map1 = new Job1Mapper();
		String foundID = map1.extractWayID(wayInputLine);
		assertTrue(foundID.equals(wayInputLineWayID));
	}

	/**
	 * Test method for {@link cs5621.project2.job1.Job1Mapper#extractNodeIndex(java.lang.String)}.
	 */
	@Test
	public void testExtractNodeIndex() {
		Job1Mapper map1 = new Job1Mapper();
		int foundIndex = map1.extractNodeIndex(wayInputLine);
		assertTrue(foundIndex == wayInputLineNodeIndex);
	}

	/**
	 * Test method for {@link cs5621.project2.job1.Job1Mapper#extractRoadName(java.lang.String)}.
	 */
	@Test
	public void testExtractRoadName() {
		Job1Mapper map1 = new Job1Mapper();
		String foundName = map1.extractRoadName(wayInputLine);
		assertTrue(foundName.equals(wayInputLineWayName));
	}
	
	/**
	 * Test method for {@link cs5621.project2.job1.Job1Mapper#extractHighwayDescriptor(java.lang.String)}.
	 */
	@Test
	public void testExtractHighwayDescriptor() {
		Job1Mapper map1 = new Job1Mapper();
		String foundDescriptor = map1.extractHighwayDescriptor(wayInputLine);
		assertTrue(foundDescriptor.equals(wayInputLineDescriptor));
	}

}
