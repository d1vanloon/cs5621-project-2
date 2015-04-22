/*
import java.lang.Math;

public class Calculator {

	private Math math;

	/**
	* Distance between two latitude longitude ordered pairs, in miles.
	* Params: point1[x-lat, y-long], point2[x-lat, y-long]
	* Return: float in miles ex. 72.36 
	* Details:
	* φ = latitude
	* λ = longtitude
	* d = 2 * rad * arcsin[ sqrt(sin^2(φ2-φ1 / 2)+cos(φ1)*cos(φ2)*sin^2(λ2-λ1 / 2)) ]
	/
	public float latLongDistance(float point1[2], float point2[2]) {
		float distance = 0, earthRadius=20902230.97, mile=5280; // (Meters is 6371000)
		 
		float aX = degToRadians(point1[0]);
		float aY = degToradians(point1[1]);
		float bX = degToRadians(point2[0]);
		float bX = degToRadians(point2[1];)
		
		distance = 2*earthRadius*math.asin(
									math.sqrt(
										haversin(bX, aX)
										+( 
											math.cos(aX) * math.cos(bX) * haversin(bY, aY) 
										)
									)
								);
		
		return distance; 
	}
	
	/*
	* Calculate haversin.
	* Params: two φ, or λ of different two points
	* Return float
	/
	private float haversin(float b, float a) {
		float hav;
		
		hav = math.pow(math.sin((b-a)/2),2); 
		
		return hav;
	}
	
	/*
	*
	/
	private float degToRadians () {
		
	}
}
*/

