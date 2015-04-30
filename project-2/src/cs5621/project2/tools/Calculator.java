package cs5621.project2.tools;

import java.io.Serializable;
import java.lang.Math;

import org.apache.hadoop.util.hash.Hash;

/**
 * @author Brad Cutshall
 */
public class Calculator implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public Integer classHash = new Integer(0);
	
	public Calculator() {
		classHash = hashCode();
	}
	
    /**
    * Distance between two latitude longitude ordered pairs, in miles.
    * For North latitude and East longitude it is a plus,
    * for South latitude and West longitute it is a minus.
    * Params: point1[x-lat, y-long], point2[x-lat, y-long]
    * Return: float in miles ex. 72.36 
    * Details:
    * φ = latitude
    * λ = longtitude
    * d = 2 * rad * arcsin[ sqrt(sin^2(φ2-φ1 / 2)+cos(φ1)*cos(φ2)*sin^2(λ2-λ1 / 2)) ]
    */
    public double latLongDistance(double point1[], double point2[]) {
        double distance = 0;
        double earthRadius=20902230.97; // Currently feet, Meters is 6371000
        double mile=5280; 
        
        double aX = toRadians(point1[0]);
        double aY = toRadians(point1[1]);
        double bX = toRadians(point2[0]);
        double bY = toRadians(point2[1]);
        double bXaXhaversin = this.haversin(bX, aX);
        double bYaYhaversin = this.haversin(bY, aY);

        distance = 2*earthRadius*Math.asin (
                Math.sqrt(
                    bXaXhaversin
                    +( 
                            Math.cos(aX) * Math.cos(bX) * bYaYhaversin 
                    )
                )
            );
        return (distance/mile); 
    }

    /*
    * Calculate haversin.
    * Params: two φ, or λ of different two points
    * Return float
    */
    public double haversin(double b, double a) {
        double hav;
        hav = Math.sin((b-a)/2);
        hav = Math.pow(hav,2); 
        return hav;
    }

    /*
    * Simple enough, convert decimal degrees to radians.
    */
    public double toRadians (double point) {
        
        return point * (Math.PI / 180);
    }
}


