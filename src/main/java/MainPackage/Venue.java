package MainPackage;

public class Venue {
    private String Name;
    private double lat;
    private double lon;

    public Venue(String name, double lat, double lon) {
        Name = name;
        this.lat = lat;
        this.lon = lon;
    }

    public String getName() {
        return Name;
    }

    public double getLat() {
        return lat;
    }

    public double getLon() {
        return lon;
    }

    public double distanceToThis(double geolat, double geolon){
        double a = (lat-geolat)*distPerLat(lat);
        double b = (lon-geolon)*distPerLng(lon);
        return Math.sqrt(a*a+b*b);
    }
    private static double distPerLng(double lat){
        return 0.0003121092*Math.pow(lat, 4)
                +0.0101182384*Math.pow(lat, 3)
                -17.2385140059*lat*lat
                +5.5485277537*lat+111301.967182595;
    }

    private static double distPerLat(double lat){
        return -0.000000487305676*Math.pow(lat, 4)
                -0.0033668574*Math.pow(lat, 3)
                +0.4601181791*lat*lat
                -1.4558127346*lat+110579.25662316;
    }
}
