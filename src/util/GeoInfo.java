package util;

import java.util.HashMap;

import datatype.CellSite;
import datatype.Meta;
import codewordsgenerator.CodeWordsGenerator.fType;

public class GeoInfo {
	StationParser sp;
	
	public GeoInfo(StationParser sp) {
		this.sp = sp;
	}
	
	public void getGeoInfo(Meta[] p, float[] in, double latitude, double longitude) {

		HashMap<String, Long> time = new HashMap<String, Long>();//key是经纬度，value是从开始到当前的时间间隔
		
		double valid_movedis = 0;
		double valid_time = 0;
		double timecount = 0;
		
		double moveratio=0;
		double moveratiotime=0;
		
		for (int i = 1; i < p.length; i++) {
			if (p[i].site > -1 && p[i-1].site > -1) {
				
				if(p[i].site == p[i-1].site) {
					CellSite cs = sp.site.get(p[i].site);
					valid_time += (p[i].time - p[i-1].time);
					String lockey = cs.latitude + "," + cs.longitude;
					if(time.containsKey(cs.latitude + "," + cs.longitude)) {
						time.put(lockey, time.get(lockey) + (p[i].time - p[i-1].time));
					} else
						time.put(lockey, (p[i].time - p[i-1].time));
					double mrdis=GPS2Dist.distance(cs.latitude, cs.longitude, latitude, longitude);
					moveratio+=mrdis*(p[i].time - p[i-1].time);
					moveratiotime+= (p[i].time - p[i-1].time);
					timecount += (p[i].time - p[i-1].time);
				} else {
					CellSite cs0 = sp.site.get(p[i-1].site);
					CellSite cs1 = sp.site.get(p[i].site);
					String lockey = (cs0.latitude + cs1.latitude) / 2 + "," + (cs0.longitude + cs1.longitude) / 2;
					if(time.containsKey(lockey)) {
						time.put(lockey, time.get(lockey) + (p[i].time - p[i-1].time));
					} else
						time.put(lockey, (p[i].time - p[i-1].time));
					
					if(p[i].time - p[i-1].time == 0)
						continue;
					
					timecount += (p[i].time - p[i-1].time);
					
					double dis = GPS2Dist.distance(cs0.latitude, cs0.longitude, cs1.latitude, cs1.longitude);
					double speed = dis / ((p[i].time - p[i-1].time) / 60.0 / 60.0 / 1000.0);
					if(speed > 300) // skip the speed over 200km/h
						continue;
					if(dis>30)
						continue;
					valid_movedis += dis;
					valid_time += (p[i].time - p[i-1].time);
					
					double mrdis0=GPS2Dist.distance(cs0.latitude, cs0.longitude, latitude, longitude);
					double mrdis1=GPS2Dist.distance(cs1.latitude, cs1.longitude, latitude, longitude);
					moveratio+=(mrdis1+mrdis0)/2*(p[i].time - p[i-1].time);
					moveratiotime+= (p[i].time - p[i-1].time);
					
				}
				
			}
		}//for
		
		in[fType.TDS.ordinal()] = (float)(valid_time / 60.0 / 60.0 / 1000.0);
		
		in[fType.MoveDis.ordinal()] = (float) (Math.log( valid_movedis+1)/Math.log(2))  ;
		if(valid_time > 0)
		{
			in[fType.AveSpeed.ordinal()] = (float) (valid_movedis / (valid_time / 60.0 / 60.0 / 1000.0));
			in[fType.AveSpeed.ordinal()]=(float) (Math.log( 1+in[fType.AveSpeed.ordinal()])/Math.log(2))  ;
		}
		else
			in[fType.AveSpeed.ordinal()] = 0;
		
		String[] str = time.keySet().toArray(new String[0]);
		for(int i=0; i<str.length; i++) {
			String[] loc = str[i].split(",");//把经纬度切分开
			double ratio = (time.get(str[i]) / timecount);//这一段占总时间的比例
			in[fType.AveLocX.ordinal()] += Float.parseFloat(loc[1]) * ratio;
			in[fType.AveLocY.ordinal()] += Float.parseFloat(loc[0]) * ratio;
			in[fType.AveLocXS.ordinal()] += Float.parseFloat(loc[1]) * ratio;
			in[fType.AveLocYS.ordinal()] += Float.parseFloat(loc[0]) * ratio;
		}
		
		in[fType.MoveRatio.ordinal()]=(float)(moveratio/moveratiotime);
		in[fType.MoveRatioS.ordinal()]=(float)(moveratio/moveratiotime);
		if(moveratiotime==0){
			CellSite cell0 = sp.site.get(p[0].site);
			CellSite cell1 = sp.site.get(p[1].site);
			double dis0=GPS2Dist.distance(cell0.latitude, cell0.longitude, latitude, longitude);
			double dis1=GPS2Dist.distance(cell1.latitude, cell1.longitude, latitude, longitude);
			in[fType.MoveRatio.ordinal()]=(float)((dis0+dis1)/2.0);
			in[fType.MoveRatioS.ordinal()]=(float)((dis0+dis1)/2.0);
		}

		in[fType.MoveRatio.ordinal()]= (float)(  Math.log(in[fType.MoveRatio.ordinal()]+1)/Math.log(2)   );
		in[fType.MoveRatioS.ordinal()]= (float)(  Math.log(in[fType.MoveRatioS.ordinal()]+1)/Math.log(2)   );

		//rg
		double sum_rg=0;
		for(int i=0;i<p.length;i++)
		{
			CellSite cs = sp.site.get(p[i].site);
			double dist=GPS2Dist.distance(cs.latitude, cs.longitude, in[fType.AveLocY.ordinal()], in[fType.AveLocX.ordinal()]);//到轨迹中心点的距离
			sum_rg+=dist*dist;	//半径的平方
		}
		in[fType.Rg.ordinal()]=(float)Math.sqrt(sum_rg/p.length);
		in[fType.Rg.ordinal()]= (float)(  Math.log(in[fType.Rg.ordinal()]+1)/Math.log(2)   );
	}
}
