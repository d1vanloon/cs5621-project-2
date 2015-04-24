package cs5621.project2.tools;

import java.lang.Math;

/**
 * @author Brad Cutshall
 */
public class Calculator {

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
        double earthRadius=20902230.97; // Meters is 6371000
        double mile=5280; 

        double aX = degToRadians(point1[0]);
        double aY = degToRadians(point1[1]);
        double bX = degToRadians(point2[0]);
        double bY = degToRadians(point2[1]);

        distance = 2*earthRadius*Math.asin
        (
            Math.sqrt(
                haversin(bX, aX)
                +( 
                        Math.cos(aX) * Math.cos(bX) * haversin(bY, aY) 
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
    private double haversin(double b, double a) {
        double hav;

        hav = Math.pow(Math.sin((b-a)/2),2); 

        return hav;
    }

    /*
    * Simple enough, convert decimal degrees to radians.
    */
    private double degToRadians (double point) {
        
        return point * (Math.PI / 180);
    }
}


