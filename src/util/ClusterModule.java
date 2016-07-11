package util;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import datatype.KmeansData;
import datatype.Element;

public class ClusterModule {

	public double[][] points;
	public double[][] points2;
	public int[] timevector;
	public KmeansData data;
	public KmeansData data2;
	double criteria = 5e-6;
	int attempts = 1000;//尝试
	public int clusternum = 20;//聚20个类
	public int clusternum2 = 20;
	public int totalClusternum = clusternum;//总的簇数=clusternum
	public double[][] range;
	
	int nearc = 8;
	
	//fusion点属于附近类的概率
	
	public void dataPrepare(Element[] e, int validtrajcount, int validframecount) {
		timevector= new int[validframecount];
		int vectorlength=0;
		int flag=0;//用于标记特征向量是否为空
		for(int j=0;j<e.length;j++)
		{
			if(flag==1)
				break;
	 	    for(int i=0;i<e[j].featureVector.length;i++)
		    {
		 	    if(e[j].featureVector[i]!=null)
			    {
				    vectorlength=e[j].featureVector[i].length;
				    flag=1;
				    break;
			    }
		    }
		}
		points = new double[validframecount][vectorlength];//validframecount为轨迹的段数，vectorlength为几个特征
		int pindex = 0;
		
		range = new double[vectorlength][2];//这里的2指范围的开始与结束，即上下限   指不同类的值的上下限，第一维指的是特征数
		
		for(int i=0; i<e.length; i++) {  //number of users
			
			for(int j=0; j<e[i].featureVector.length; j++) {//number of frames
				if(e[i].featureVector[j] != null) {
					timevector[pindex]=j;//time
					for(int k=0; k<e[i].featureVector[j].length; k++) {
						points[pindex][k] = e[i].featureVector[j][k];
						
						if(pindex == 0) {
							range[k][0] = range[k][1] = points[pindex][k];
						} else {//不断更新范围
							if(points[pindex][k] > range[k][1])
								range[k][1] = points[pindex][k];
							else if(points[pindex][k] < range[k][0])
								range[k][0] = points[pindex][k];
						}
						
					}//for
					pindex++;
				}
			}//for number of frames
		}//for number of users
		
		pindex = 0;
		for(int i=0; i<e.length; i++) {
			for(int j=0; j<e[i].featureVector.length; j++) {
				if(e[i].featureVector[j] != null) {
					for(int k=0; k<e[i].featureVector[j].length; k++) {
						points[pindex][k] = (points[pindex][k] - range[k][0]) / (range[k][1] - range[k][0]);
					}
					pindex++;
				}
			}
		}
		
	}
	
	/**
	 * @param e
	 * @param validtrajcount
	 * @param validframecount
	 */
	public void cluster(Element[] e, int validtrajcount, int validframecount) {
		
		dataPrepare(e, validtrajcount, validframecount);
		
		System.out.println("data prepared");
		
		long t1 = System.currentTimeMillis();
		
		data = new KmeansData(points, points.length, points[0].length); //对应构造函数public KmeansData(double[][] data, int length, int dim)
		KmeansParam param = new KmeansParam(); 
		param.criteria = this.criteria;
		param.attempts = this.attempts;
		param.initCenterMehtod = KmeansParam.CENTER_RANDOM; 
		

		Kmeans.doKmeans(clusternum, data, param);

		System.out.println("cost: " + (System.currentTimeMillis()-t1) / 1000.0);
		
		clusterWriter("cluster_" + clusternum + ".csv", data.dim, data.centerCounts, data.centers);//写文件，得到cluster_20.csv和ClusterCenter20
		clusterFusion(e, data);
		
		System.out.println();
		
	}
	
	public void clusterFusion(Element[] e, KmeansData data) {//点属于附近聚类的概率
		int pindex = 0;
		int startindex = totalClusternum - clusternum;//开始的下标
		
		for(int i=0; i<e.length; i++) {
			
			float[][] vtemp = new float[e[i].featureVector.length][clusternum];
			for(int j=0; j<e[i].featureVector.length; j++) {
				if(e[i].featureVector[j] != null) {
					
					if(e[i].value != null)
						System.arraycopy(e[i].value[j], 0, vtemp[j], 0, e[i].value[j].length);
					
					int belong = data.labels[pindex];
					
					double[] dis = new double[data.centers.length];
					HashSet<Double> temp = new HashSet<Double>();
					for(int k=0; k<data.centers.length; k++) {
						dis[k] = dist(points[pindex], data.centers[k], data.centers[k].length);
						temp.add(dis[k]);
					}
					Double[] hashset = temp.toArray(new Double[0]);
					Arrays.sort(hashset);
					
					int near_c = nearc;//附近的c个聚类，20个中最近的c个类
					if(hashset.length < near_c)
						near_c = hashset.length;
					float total = 0;
					for(int k=0; k<dis.length; k++) {
						if(dis[k] <= hashset[near_c-1]) {
							vtemp[j][startindex+k] = (float) KERNEL_NORMAL(dis[k], 0, 0.1f);
							total += vtemp[j][startindex+k];
						}
					}
					
					for(int k=0; k<dis.length; k++) {
						if(total == 0) {
							vtemp[j][startindex+k] = 0.0f;
							continue;
						}
						vtemp[j][startindex+k] /= total;
					}
					
					pindex++;
				} else {
					vtemp[j] = null;
				}
			}
			
			e[i].value = vtemp;
		}
	}
	
	public double dist(double[] pa, double[] pb, int dim) {
        double rv = 0;
        for (int i = 0; i < dim; i++) {
        	if(pa[i] == -1 || pb[i] == -1)
        		continue;
            double temp = pa[i] - pb[i];
            temp = temp * temp;
            rv += temp;
        }
        return Math.sqrt(rv);
    }
	
	public static double KERNEL_NORMAL(double x, float mean, float var) {
		return (0.3989422804 / var)
				* Math.exp(-0.5 * (x - mean) * (x - mean) / (var * var));
	}
	
	public void clusterWriter(String path, int dim, int[] centerCounts, double[][] centers) {
		try {
			DataOutputStream datawriter = new DataOutputStream(new FileOutputStream("data/ClusterCenter"+centerCounts.length));
			 datawriter.writeInt(centerCounts.length);
			 datawriter.writeInt(centers[0].length);
			 for(int i=0;i<centerCounts.length;i++)
			 {
				 datawriter.writeInt(centerCounts[i]);
				 for(int j=0; j<centers[i].length; j++) 
				 {
					 datawriter.writeFloat((float)centers[i][j]);
				 }
			 }
			 datawriter.close();
			
			
			FileWriter w = new FileWriter("data/" + path);
			
			w.write("cluster:" + clusternum + " feature:" + dim + "\n");
			
			for(int i=0; i<centerCounts.length; i++) {
				w.write(centerCounts[i] + " ");
				StringBuffer sb = new StringBuffer();
				for(int j=0; j<centers[i].length; j++) {
					sb.append(Double.toString(centers[i][j]) + ",");
				}
				sb.delete(sb.length()-1, sb.length());
				w.write(sb.toString() + "\n");
			}
			w.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
