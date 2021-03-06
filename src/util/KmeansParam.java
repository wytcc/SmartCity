package util;

public class KmeansParam {
	public static final int CENTER_ORDER = 0;
	public static final int CENTER_RANDOM = 1;
	public static final int MAX_ATTEMPTS = 4000;
	public static final double MIN_CRITERIA = 1.0;
	
	public double criteria = MIN_CRITERIA; //被修改过的中心点最大修改量的阈值
	public int attempts = MAX_ATTEMPTS; 
	public int initCenterMehtod = CENTER_ORDER; 
	public boolean isDisplay = true; 
}
