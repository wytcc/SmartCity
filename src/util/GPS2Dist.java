package util;

public class GPS2Dist {
	private final static double EARTH_RADIUS = 6378.137;

	public static double deg2rad(double degree) {
		return degree / 180 * Math.PI;
	}

	// 将弧度转换为角度
	public static double rad2deg(double radian) {
		return radian * 180 / Math.PI;
	}

	// 球面余弦公式
	public static double distance(double lat1, double lon1, double lat2,
			double lon2) {
		if (lat1 - lat2 == 0 && lon1 - lon2 == 0)
			return 0.0;
		double theta = lon1 - lon2;
		double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2))
				+ Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2))
				* Math.cos(deg2rad(theta));
		dist = Math.acos(dist);
		dist = rad2deg(dist);
		double miles = dist * 60 * 1.1515 * 1.609344;// km
		return miles;
	}

	// Haversine
	public static double distance2(double lat1, double lon1, double lat2,
			double lon2) {
		double radLat1 = deg2rad(lat1);
		double radLat2 = deg2rad(lat2);
		double a = radLat1 - radLat2;
		double b = deg2rad(lon1) - deg2rad(lon2);
		double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
				+ Math.cos(radLat1) * Math.cos(radLat2)
				* Math.pow(Math.sin(b / 2), 2)));
		s = s * EARTH_RADIUS;
		// s = Math.round(s * 10000.0) / 10000.0;
		return s;
	}

	// 极坐标法
	public static double angleRad(double lat1, double lon1, double lat2,
			double lon2) {
		double x = Math.sin(deg2rad(lon2 - lon1)) * Math.cos(deg2rad(lat2));
		double y = Math.cos(deg2rad(lat1)) * Math.sin(deg2rad(lat2))
				- Math.sin(deg2rad(lat1)) * Math.cos(deg2rad(lat2))
				* Math.cos(deg2rad(lon2 - lon1));
		double A = Math.atan2(x, y);
		// double angle = rad2deg(A);
		// return angle % 360;
		return A;
	}

	public static double angleDeg(double lat1, double lon1, double lat2,
			double lon2) {
		double angle = rad2deg(angleRad(lat1, lon1, lat2, lon2));
		return angle % 360;
	}

	public static void main(String[] input) {
		double dis = distance(127.3, 27.3, 127.3, 27.3001);
		//double diss = distance(127.3, 27.3, 127.3001, 27.3);
		double dis1 = distance(127.3, 27.3, 127.3001, 27.3);
		System.out.println(dis + " " + dis1);
		double dis2 = distance2(127.3, 27.3, 127.3, 27.3001);
		System.out.println(dis2);
		System.out.println();
	}
}
