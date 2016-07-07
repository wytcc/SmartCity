package util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;

import datatype.Meta;

public class ResidenceDistribution {
	
	public long[][] time_slice;
	
	StationParser sp;
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	public ResidenceDistribution(StationParser sp) {
		this.sp = sp;
		timeRange();
	}
	
	public void timeRange() {
		System.out.println("residence time range: ");
		
		try {

			long stime = sdf.parse("2014-1-21 00:00:00.00").getTime();
			long etime = sdf.parse("2014-1-28 00:00:00.00").getTime();
			int day = (int) ((etime - stime) / 86400000);//计算天数
			
			time_slice = new long[day][2];
			
			int hour = 60 * 60 * 1000;//1小时里面的毫秒数
			
			int s_hour = 0;// start hour 以小时为单位   0点
			int e_hour = 6;// end hour            6点
			
			for(int i=0; i<day; i++) {
				time_slice[i][0] = stime + i * hour * 24 + s_hour * hour; //每天的开始时间
				time_slice[i][1] = stime + i * hour * 24 + e_hour * hour; //每天的结束时间  （从0点到6点）
				
				System.out.print(sdf.format(time_slice[i][0]) + " to ");
				System.out.println(sdf.format(time_slice[i][1]));
			}
			
			System.out.println();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public float[][] residenceJudge(Meta[] p) {//0:00-6:00的版本
		
		int[][] tag = new int[time_slice.length][2];
		for(int i=0; i<time_slice.length; i++) {//every day i
			for(int j = 0; j < p.length; j++) {//一个人的不同数据
				if(p[j].time <= time_slice[i][0]) {
					tag[i][0] = tag[i][1] = j;
				} else if(p[j].time >= time_slice[i][1]) {
					tag[i][1] = j;
					break;
				} else if(time_slice[i][0] < p[j].time && time_slice[i][1] > p[j].time) {
					tag[i][1] = j;
				}
			}
		}
		HashMap<Integer, Long> temp = new HashMap<Integer, Long>();
		long total = 0;
		for(int i=0; i<tag.length; i++) {
			if(tag[i][1] > tag[i][0]) {//去掉了0:00-6:00没有记录的情况

				for(int j=tag[i][0]; j<=tag[i][1]; j++) {//头和尾

					if(time_slice[i][0] < p[j].time) {
						if(j >= 1) {

							int siteloc=p[j-1].site;
							if(siteloc > -1) {
								if(temp.get(siteloc)==null)
									temp.put(siteloc, 0L);
								Long t = temp.get(siteloc);
								long ftime=0L;
								long ltime=0L;
								
								if((j-1)==tag[i][0]&&p[j-1].time<time_slice[i][0])
								{
									ftime=time_slice[i][0];

								}
								else
									ftime=p[j-1].time;
								if(j==tag[i][1]&&p[j].time>time_slice[i][1])
								{
									ltime=time_slice[i][1];

								}
								else
								{
									ltime=p[j].time;

								}
								t+=ltime-ftime;
								total+=ltime-ftime;
								if(t!=0L)
									temp.put(siteloc, t);
							}

						}
					}
				}
			}
		}
		
		if(temp.size() > 0) {
			float[][] ret = new float[2][temp.size()];
			Integer[] keys = temp.keySet().toArray(new Integer[0]);
			for(int i=0; i<ret[0].length; i++) {
				ret[0][i] = keys[i];
				ret[1][i] = (float) ((double)temp.get(keys[i]) / total);
			}
			return ret;
		} else
		{
			return null;
		}
	}
	
	
	public static void main(String[] in) {
		StationParser sp = new StationParser();
		ResidenceDistribution rd = new ResidenceDistribution(sp);
		System.out.println();
	}
}
