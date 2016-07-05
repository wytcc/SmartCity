package datatype;

public class CellSite {
	public int id = -1;
	public double longitude = -1000;
	public double latitude = -1000;
	public double[] edgeX = null;
	public double[] edgeY = null;
	public int[] neighbor = null;
	public String[] baseStation = null;
	public String[] poi = null;
	public int[] poi_type_l1 = null;
	
	public CellSite() {}
	
	public String generateLocationId() {
		return String.valueOf(id);
	}
	
	public String toString() {
		return id + ", " +
				longitude + ", " + latitude;
	}
}
