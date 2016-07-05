package util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;

import datatype.Station;
import datatype.CellSite;
import util.SQLModule;

public class StationParser {
	
	private String typepath = "conf/TypeList.txt";
	
	public SQLModule sm = new SQLModule();
	public Statement stmt;
	
	public HashMap<String, Station> station = new HashMap<String, Station>();
	public HashMap<Integer, CellSite> site = new HashMap<Integer, CellSite>();
	
	public static HashMap<String, Integer> typemap = new HashMap<String, Integer>();
	public static HashMap<String, Integer> typeindex = new HashMap<String, Integer>();
	
	public double maxLongitude, minLongitude, maxLatitude, minLatitude;
	public double boundrange = 0.001;
	
	public StationParser(){
		sm.connect();
		try {
			stmt = sm.conn.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		readType();
		readBaseStation();
		readSite();
		
		sm.disconnect();
		
		System.out.println("read completed");
	}
	
	public void readBaseStation() {
		String tableName = "mobiledata" + "." + "base_station";//����
		String sql = "select * from " + tableName;
		
		try {
			ResultSet res = stmt.executeQuery(sql);
			while (res.next()) {
				Station ret = new Station();
				ret.LAC = res.getInt(1);
				ret.CELL = res.getInt(2);
				ret.site_map = res.getInt(3);//site_index
				ret.longitude = res.getFloat(4);
				ret.latitude = res.getFloat(5);
				ret.direction = res.getFloat(6);
				ret.cityname = res.getString(7);
				ret.areaname = res.getString(8);
				ret.description = res.getString(9);
				
				if(!locationValidate(ret.longitude, ret.latitude)) {
					ret.site_map = -1;
				}
				
				station.put(ret.LAC + "," + ret.CELL, ret);
		}
			} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Station[] s = station.values().toArray(new Station[0]);//��station��ֵȡ����ת��ΪStation����
		for(int i=0; i<s.length; i++) {
			
		}
	}
	
	public void readSite() {
		String tableName = "mobiledata" + "." + "site";//����
		String sql = "select * from " + tableName;
		
		try {
			ResultSet res = stmt.executeQuery(sql);
			while (res.next()) {
				CellSite ret = new CellSite();
				ret.id = res.getInt(1);
				ret.longitude = res.getFloat(2);
				ret.latitude = res.getFloat(3);
				
				String vertice = res.getString(4);//vertice ��  
				String neighbor = res.getString(5);//neighbor
				String bs = res.getString(6);//base_station ��վ
				Blob poi = res.getBlob(7);//Blob�Ƕ����ƴ����
				int poi_size = res.getInt(8);
				String poi_type_l1 = res.getString(9);
				
				String[] str = vertice.split(";");//121.2390591892921,30.19479826133926;�ָ��õ���
				ret.edgeX = new double[str.length];//��һ���ĵ�ĸ���
				ret.edgeY = new double[str.length];
				for(int i=0; i<str.length; i++) {
					String[] temp = str[i].split(",");//�ѵ�������X Y�ָ����ֱ���
					ret.edgeX[i] = Float.valueOf(temp[0]);
					ret.edgeY[i] = Float.valueOf(temp[1]);
				}
				
				str = neighbor.split(";");
				ret.neighbor = new int[str.length];//�������ھӵĸ���
				for(int i=0; i<str.length; i++) {
					ret.neighbor[i] = Integer.valueOf(str[i]);//�������ݿ��еõ���String���ݽ�������ת�����ٴ������ݽṹ��
				}
				
				str = bs.split(";");
				ret.baseStation = new String[str.length];
				for(int i=0; i<str.length; i++) {
					ret.baseStation[i] = str[i];//��վλ��
				}
				
				ret.poi = new String[poi_size];
				InputStream ins = poi.getBinaryStream();
				byte[] b = new byte[25];
				int count = 0;
				while(ins.read(b) != -1) {
					String temp = new String(b);
					ret.poi[count] = temp.substring(0, temp.length()-1);//ȡ�Ӵ���ȥ�����һ���ַ�
					b = new byte[25];
					count++;
				}
				
				str = poi_type_l1.split(";");
				ret.poi_type_l1 = new int[str.length];
				for(int i=0; i<str.length; i++)
					ret.poi_type_l1[i] = Integer.parseInt(str[i]);//����ת��
				
				site.put(ret.id, ret);//������һ�������Ժ󣬽�cellsite����site���hashmap�У�key��id��value��cellsite
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		CellSite[] temp = site.values().toArray(new CellSite[0]);//��ȡ�����е�ֵ����ת��ΪCellSite����
		for(int i=0; i<temp.length; i++) {
			if(locationValidate(temp[i].longitude, temp[i].latitude)) {
				if(!locationValidate(maxLongitude, maxLatitude) || !locationValidate(minLongitude, minLatitude)) {//�൱����һ����ʼ��
					maxLongitude = minLongitude = temp[i].longitude;
					maxLatitude = minLatitude = temp[i].latitude;
				}
				
				if(temp[i].longitude > maxLongitude) maxLongitude = temp[i].longitude;
				else if(temp[i].longitude < minLongitude) minLongitude = temp[i].longitude;
				
				if(temp[i].latitude > maxLatitude) maxLatitude = temp[i].latitude;
				else if(temp[i].latitude < minLatitude) minLatitude = temp[i].latitude;
			}
		}
		maxLongitude += boundrange;//��
		minLongitude -= boundrange;
		maxLatitude += boundrange;
		minLatitude -= boundrange;
		
		System.out.println();
	}
	
	public void readType() {

		try {
			HashMap<String, String> temp = new HashMap<String, String>();
			String relativelyPath = System.getProperty("user.dir");//����û���ǰ����Ŀ¼
			typepath = relativelyPath + "/" + typepath;
			
			//���ļ�
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new FileInputStream(typepath)));
			String data = null;
			while ((data = br.readLine()) != null) {
				String[] str = data.split("/");
				temp.put(data, str[0]);//��������һ��������/�ָ����hashmap key��һ������ value����һ���ַ���
			}
			br.close();
			HashSet<String> type_set = new HashSet<String>(temp.values());//ֵ
			String[] type_str = type_set.toArray(new String[0]);//ת��ΪString�������� ��⣺ http://blog.csdn.net/ystyaoshengting/article/details/50697783
			for (int i = 0; i < type_str.length; i++) {
				typeindex.put(type_str[i], i);
			}

			String[] type_key = temp.keySet().toArray(new String[0]);//��
			for (int i = 0; i < type_key.length; i++) {
				typemap.put(type_key[i],
						typeindex.get(type_key[i].split("/")[0]));
			}

			System.out.println();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Station getStation(int lac, int cell) {

		return station.get(lac + "," + cell);
	}
	
	public double absolutePosX(double rel_longitude) {
		return rel_longitude * (maxLongitude - minLongitude) + minLongitude;
	}
	
	public double absolutePosY(double rel_latitude) {
		return rel_latitude * (maxLatitude - minLatitude) + minLatitude;
	}
	
	public double relativePosX(double ab_longitude) {
		return (ab_longitude - minLongitude) / (maxLongitude - minLongitude);
	}
	
	public double relativePosY(double ab_latitude) {
		return (ab_latitude - minLatitude) / (maxLatitude - minLatitude);
	}
	
	public static boolean locationValidate(double longitude, double latitude) {//�ж���γ���Ƿ�Ϸ�
		if(longitude == 0.0 && latitude == 0.0)
			return false;
		
		if(longitude >= -180.0 && longitude <= 180.0 
				&& latitude >= -90.0 && latitude <= 90.0)
			return true;
		else return false;
	}
	
	public static void main(String[] str) {
		StationParser sp = new StationParser();
		sp.getStation(22384, 10186);
		System.out.println();
	}
}

