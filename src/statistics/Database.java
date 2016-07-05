package statistics;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import preconditioning.Preconditioning;
import util.GPS2Dist;
import util.SQLModule;

public class Database {

	public SQLModule sm = new SQLModule();
	public Statement stmt;
	public PreparedStatement ps;
	
	public void computeDistance(){
		
		try{
			FileWriter fw = new FileWriter("distanceBetweenStation.csv");
			String header = "station-start,station-end,distance\r\n";
			fw.write(header);
			fw.flush();
			fw.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		
		HashMap<String,Double> distanceMap = new HashMap<String,Double>();
		
		sm.connect();
		try {
			stmt = sm.conn.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//first query
		String tableName = "mobiledata" + "." + "site";//表名
		String sql = "select id,longitude,latitude,neighbor from " + tableName + " where longitude>=120.53695 AND longitude<=120.900696 AND latitude>=27.846675 AND latitude<=28.162664";
		HashMap<Integer,BaseStation> firstResult = new HashMap<Integer,BaseStation>();
		
		try {
			ResultSet res = stmt.executeQuery(sql);
			while (res.next()) {
				BaseStation bs = new BaseStation();
				bs.id = res.getInt(1);
				bs.longitude = res.getDouble(2);
				bs.latitude = res.getDouble(3);
				bs.neighbor = res.getString(4);
				firstResult.put(bs.id, bs);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//second query  遍历map，将value切割然后分别
		sql = "select longitude,latitude from " + tableName + " where id=?";
		try {
			ps = sm.conn.prepareStatement(sql);
			Iterator iter0 = firstResult.entrySet().iterator();
			while (iter0.hasNext()) {
				Map.Entry entry = (Map.Entry) iter0.next();
				int id = (int)entry.getKey();
				BaseStation bs0 = (BaseStation)entry.getValue();
				String nei = bs0.neighbor;
				String[] neighbors = nei.split(";");
				for (int i = 0; i < neighbors.length; i++){
					int anotherId = Integer.parseInt(neighbors[i]);
					ps.setInt(1, anotherId);
					ResultSet res = ps.executeQuery();
					BaseStation bs = new BaseStation();
					while (res.next()) {
						bs.id = anotherId;
						bs.longitude = res.getDouble(1);
						bs.latitude = res.getDouble(2);
						
					}
					double dis = GPS2Dist.distance(bs0.latitude, bs0.longitude, bs.latitude,
							bs.longitude);
					String key = bs0.id + "," + bs.id;
					String key1 = bs.id + "," + bs0.id;
					double value = dis;
					if(distanceMap.containsKey(key) || distanceMap.containsKey(key1)){
						continue;
					}
					distanceMap.put(key, value);
				}
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//将计算得到的数据写入excel文件
		try{
			FileWriter fw = new FileWriter("distanceBetweenStation.csv", true);
					
			StringBuffer s = new StringBuffer();

			Iterator iter = distanceMap.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry entry = (Map.Entry) iter.next();
				String key = (String)entry.getKey();
				double val = (double)entry.getValue();
				s.append(key+","+val+"\r\n");
			}
					
				fw.write(s.toString());
				fw.flush();
				fw.close();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		Database program = new Database();
		program.computeDistance();
		System.out.println("程序运行结束");
	}
}
