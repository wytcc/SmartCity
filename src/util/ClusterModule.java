package util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import datatype.Element;
import datatype.KmeansData;

public class ClusterModule {

//	public double[][] points;
//	public double[][] points2;
	public double[][] stopPoints;
	public double[][] movePoints;
	public int stopClusterNum = 10;//静止的点聚10个类
	public int moveClusterNum = 10;//移动的点聚10个类
	public int validFrameCountStop = 0;
	public int validFrameCountMove = 0;
//	public int[] timevector;
//	public KmeansData data;
//	public KmeansData data2;
	public KmeansData dataStop;
	public KmeansData dataMove;
	double criteria = 5e-6;
	int attempts = 1000;//尝试
	public int clusternum = 20;//聚20个类
//	public int clusternum2 = 20;
	public int totalClusternum = clusternum;//总的簇数=clusternum
	public double[][] rangeStop;
	public double[][] rangeMove;
	public HashMap<Integer, Integer> stopDataMap = new HashMap<Integer, Integer>();//key指的是对应的validFrameCount，即片段的id，value指的是对应的data数组中的下标
	public HashMap<Integer, Integer> moveDataMap = new HashMap<Integer, Integer>();
	public int[] stopIndex;
	public int[] moveIndex;
	
	int nearc = 5;//取附近最近的5个类
	
	//fusion点属于附近类的概率
	
	public void dataPrepare(Element[] e, int validtrajcount, int validframecount) {
//		timevector= new int[validframecount];
		int stopVectorLength = 4;
		int moveVectorLength = 8;
		int flag=0;//用于标记特征向量是否为空
		
		for(int j=0;j<e.length;j++)
		{
			if(flag==1)
				break;
	 	    for(int i=0;i<e[j].featureVector.length;i++)
		    {
		 	    if(e[j].featureVector[i]!=null)
			    {
		 	    	if (e[j].moveornot[i]){
		 	    		moveVectorLength = 8;//运动轨迹有8个特征
		 	    	}else {
		 	    		stopVectorLength = 4;//静止轨迹有4个特征
		 	    	}
				    flag=1;
				    break;
			    }
		    }
		}
		
		//统计动轨迹和静轨迹数
		for (int j = 0; j < e.length; j++){
			for (int i = 0; i < e[j].featureVector.length; i++){
				if(e[j].featureVector[i] != null)
				{
					if(e[j].moveornot[i]){
						validFrameCountMove++;
					}else {
						validFrameCountStop++;
					}
				}
			}
		}
		
		
//		points = new double[validframecount][vectorlength];//validframecount为轨迹的段数，vectorlength为几个特征
		stopPoints = new double[validFrameCountStop][stopVectorLength];//存储所有静止轨迹的数据
		movePoints = new double[validFrameCountMove][moveVectorLength];//运动轨迹数据
		int pindexS = 0;
		int pindexM = 0;
		stopIndex = new int[validFrameCountStop];
		moveIndex = new int[validFrameCountMove];
		
//		range = new double[vectorlength][2];//这里的2指范围的开始与结束，即上下限   指不同类的值的上下限，第一维指的是特征数
		rangeStop = new double[stopVectorLength][2];
		rangeMove = new double[moveVectorLength][2];
		
		for(int i=0; i<e.length; i++) {  //number of users
			
			for(int j=0; j<e[i].featureVector.length; j++) {//number of frames
				if(e[i].featureVector[j] != null) {
//					timevector[pindex]=j;//time
					if (e[i].moveornot[j]){//如果是动轨迹，就放入movePoints数组
						moveIndex[pindexM] = pindexM + pindexS;//记录对应的validFrameCount
						for (int k = 0; k < moveVectorLength; k++){//存前八个
							movePoints[pindexM][k] = e[i].featureVector[j][k];
							if (pindexM == 0){
								rangeMove[k][0] = rangeMove[k][1] = movePoints[pindexM][k];
							} else {//不断更新范围
								if(movePoints[pindexM][k] > rangeMove[k][1]){
									rangeMove[k][1] = movePoints[pindexM][k];
								}else if(movePoints[pindexM][k] < rangeMove[k][0]){
									rangeMove[k][0] = movePoints[pindexM][k];
								}
							}
						}//for
						pindexM++;
					}else {//否则，放入stopPoints数组
						stopIndex[pindexS] = pindexM+pindexS;
						for (int k = 0; k < stopVectorLength; k++){//存后四个
							stopPoints[pindexS][k] = e[i].featureVector[j][k+moveVectorLength];
							if (pindexS == 0){
								rangeStop[k][0] = rangeStop[k][1] = stopPoints[pindexS][k];
							} else {//不断更新范围
								if(stopPoints[pindexS][k] > rangeStop[k][1]){
									rangeStop[k][1] = stopPoints[pindexS][k];
								}else if(stopPoints[pindexS][k] < rangeStop[k][0]){
									rangeStop[k][0] = stopPoints[pindexS][k];
								}
							}
						}//for
						pindexS++;
					}
				}
			}//for number of frames
		}//for number of users
		
		for (int i = 0; i < 8; i++){
			for (int j = 0; j < 2; j++){
				System.out.println(rangeMove[i][j]);
			}
		}
		
		pindexS = 0;
		pindexM = 0;
//		pindex = 0;
		for(int i=0; i<e.length; i++) {
			for(int j=0; j<e[i].featureVector.length; j++) {
				if(e[i].featureVector[j] != null) {
					if (e[i].moveornot[j]){
						for (int k = 0; k < moveVectorLength; k++){
							movePoints[pindexM][k] = (movePoints[pindexM][k] - rangeMove[k][0]) / (rangeMove[k][1] - rangeMove[k][0]);
						}
						pindexM++;
					} else {
						for (int k = 0; k < stopVectorLength; k++){
							stopPoints[pindexS][k] = (stopPoints[pindexS][k] - rangeStop[k][0]) / (rangeStop[k][1] - rangeStop[k][0]);
						}
						pindexS++;
					}
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
		
		for (int i = 0; i < 8; i++){
			for (int j = 0; j < 2; j++){
				System.out.println(rangeMove[i][j]);
			}
		}
		System.out.println("data prepared");
		
		long t1 = System.currentTimeMillis();
		
//		data = new KmeansData(points, points.length, points[0].length); //对应构造函数public KmeansData(double[][] data, int length, int dim)
		dataStop = new KmeansData(stopPoints, stopPoints.length, stopPoints[0].length);
		dataMove = new KmeansData(movePoints, movePoints.length, movePoints[0].length);
		
		//validFrameCount与动静数组分别对应
		for (int i = 0; i < stopPoints.length; i++){
			stopDataMap.put(stopIndex[i], i);
		}
		for (int i = 0; i < movePoints.length; i++){
			moveDataMap.put(moveIndex[i], i);
		}
		
		KmeansParam param = new KmeansParam(); 
		param.criteria = this.criteria;
		param.attempts = this.attempts;
		param.initCenterMehtod = KmeansParam.CENTER_RANDOM; 
		
		//静轨迹聚类
		Kmeans.doKmeans(stopClusterNum, dataStop, param);
		//动轨迹聚类
		Kmeans.doKmeans(moveClusterNum, dataMove, param);

		System.out.println("cost: " + (System.currentTimeMillis()-t1) / 1000.0);
		
		clusterFusionS(e, dataStop);
		clusterFusionM(e, dataMove);
		
	}
	
	public void clusterFusionS(Element[] e, KmeansData data) {//点属于附近聚类的概率 for stop points
		int pindex = 0;
		int startindex = totalClusternum - stopClusterNum;//开始的下标
		for(int i=0; i<e.length; i++) {
			float[][] vtemp = new float[e[i].featureVector.length][totalClusternum];
			for(int j=0; j<e[i].featureVector.length; j++) {//轨迹片段数
				if(e[i].featureVector[j] != null) {
					if(e[i].value != null){//把已经存在的复制过来
						System.arraycopy(e[i].value[j], 0, vtemp[j], 0, e[i].value[j].length);
					}
					if (!e[i].moveornot[j]){
						double[] dis = new double[data.centers.length];//data.centers.length指的是聚类的个数
						HashSet<Double> temp = new HashSet<Double>();
						for(int k=0; k<data.centers.length; k++) {
							dis[k] = dist(data.data[pindex], data.centers[k], data.centers[k].length);
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
					}
				} else {
					vtemp[j] = null;
				}
			}//for 轨迹片段数
			//向前补全十个0，即先把前面的数据移到后面，然后前面填充0
//			for (int j = 0; j < vtemp.length; j++){
//				for (int k = 0; k < stopClusterNum; k++){
//					vtemp[j][k] = 0.0f;
//				}
//			}
//			System.out.println("for 补0 结束");
			e[i].value = vtemp;
		}//for e.length
	}
	
	public void clusterFusionM(Element[] e, KmeansData data) {//点属于附近聚类的概率  for move points
		int pindex = 0;
		int startindex = totalClusternum - clusternum;//开始的下标
		
		for(int i=0; i<e.length; i++) {
			float[][] vtemp = new float[e[i].featureVector.length][totalClusternum];
			for(int j=0; j<e[i].featureVector.length; j++) {//轨迹片段数
				if(e[i].featureVector[j] != null) {
					if(e[i].value != null)
						System.arraycopy(e[i].value[j], 0, vtemp[j], 0, e[i].value[j].length);
					
					if (e[i].moveornot[j]){
						double[] dis = new double[data.centers.length];//data.centers.length指的是聚类的个数
						HashSet<Double> temp = new HashSet<Double>();
						for(int k=0; k<data.centers.length; k++) {
							dis[k] = dist(data.data[pindex], data.centers[k], data.centers[k].length);
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
					}
				} else {
					vtemp[j] = null;
				}
			}//for 轨迹片段数
			
			//向后补全十个0
//			for (int j = 0; j < vtemp.length; j++){
//				for (int k = stopClusterNum; k < (stopClusterNum+10); k++){
//					vtemp[j][k] = 0.0f;
//				}
//			}
			
			e[i].value = vtemp;
		}//for e.length
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
	
}
