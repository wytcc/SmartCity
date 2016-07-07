package util;

import java.util.HashMap;

import datatype.Meta;
import codewordsgenerator.CodeWordsGenerator.fType;

public class MobilityEntropy {

	public static void entropy(Meta[] p, float[] in) {
		HashMap<String, Integer> hs = new HashMap<String, Integer>();
		HashMap<String, Integer> static_time = new HashMap<String, Integer>();
		HashMap<String, Integer> dynamic_time = new HashMap<String, Integer>();
		float count = 0;
		int site_temp = -100;
		long time_temp = 0;
		double dynamic_t = 0, static_t = 0;
		for (int i = 0; i < p.length; i++) {
			if (p[i].site > -1) {
				if (!hs.containsKey(String.valueOf(p[i].site))) {
					hs.put(String.valueOf(p[i].site), 0);
					static_time.put(String.valueOf(p[i].site), 0);
					dynamic_time.put(String.valueOf(p[i].site), 0);
				}
				
				hs.put(String.valueOf(p[i].site),
						hs.get(String.valueOf(p[i].site)) + 1);
				
				if(p[i].site == site_temp) {
					static_time.put(String.valueOf(site_temp), 
							(int) (static_time.get(String.valueOf(site_temp)) + (p[i].time-time_temp)) );
					static_t += (p[i].time-time_temp);
				} else if(site_temp >= 0){
					dynamic_time.put(String.valueOf(site_temp), 
							(int) (dynamic_time.get(String.valueOf(site_temp)) + (p[i].time-time_temp)/2));//half the stay time
					
					dynamic_time.put(String.valueOf(p[i].site), 
							(int) (dynamic_time.get(String.valueOf(p[i].site)) + (p[i].time-time_temp)/2));
					
					dynamic_t += (p[i].time-time_temp);
					if(p[i].time-time_temp<0)
					{
						System.out.print("");
					}
				}
				
				site_temp = p[i].site;
				time_temp = p[i].time;
				count++;
			}
		}

		Integer[] array = hs.values().toArray(new Integer[0]);

		String[] array1 = static_time.keySet().toArray(new String[0]);
		for (int i = 0; i < array.length; i++) {
			double time = static_time.get(array1[i]) + dynamic_time.get(array1[i]);
			if(time == 0)
				continue;
			in[fType.ETrand.ordinal()] += (float) (Math.log(time / (p[p.length-1].time - p[0].time))
					/ Math.log(2) * (time / (p[p.length-1].time - p[0].time)));
		}
		in[fType.ETrand.ordinal()] = -in[fType.ETrand.ordinal()];//entropy 1
	
		for (int i = 0; i < array.length; i++) {
			in[fType.Eunc.ordinal()] += (float) (Math.log(array[i] / count)
					/ Math.log(2) * (array[i] / count));
		}
		in[fType.Eunc.ordinal()] = -in[fType.Eunc.ordinal()];//entropy 2 may be given up
		
	}
}
