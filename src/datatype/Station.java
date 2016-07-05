package datatype;

public class Station {

	public String cityname = "";
	public String areaname = "";
	public int LAC = -1;
	public int CELL = -1;
	public String description = "";
	public double longitude = -1000;
	public double latitude = -1000;
	public double direction = -1000;
	
	public int count;
	
	public int site_map = -1;
	
	public Station() {}
	
	public String generateLocationId() {
		return LAC + "," + CELL;
	}

	public void clearCount() {
		count = 0;
	}
	
	public String toString() {
		return LAC + ", " + CELL + ", " + site_map + ", " + 
				longitude + ", " + latitude + ", " + 
				direction + ", " + cityname + ", " + 
				areaname + ", " + description;
	}
}
