package datatype;

public class Element {
	public long id;
	public float[][] value;//描述的是点属于附近聚类的概率
	public float[][] featureVector;
	
	public float[][] location;
	public float[][] locationresult;
	
	public int[] residence;
	public float[] residenceRatio;

	public Long[][] time;
	public boolean[] moveornot;
	
}