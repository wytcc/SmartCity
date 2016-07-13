package util;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import datatype.KmeansData;
import datatype.KmeansResult;



public class Kmeans {
	/**
     * double[][] 元素全置0
     * 
     * @param matrix
     *            double[][]
     * @param highDim
     *            int
     * @param lowDim
     *            int <br/>
     *            double[highDim][lowDim]
     */
    private static void setDouble2Zero(double[][] matrix, int highDim, int lowDim) {
        for (int i = 0; i < highDim; i++) {
            for (int j = 0; j < lowDim; j++) {
                matrix[i][j] = 0;
            }
        }
    }

    /**
     * 拷贝源二维矩阵元素到目标二维矩阵。 foreach (dests[highDim][lowDim] = sources[highDim][lowDim]);
     * 
     * @param dests
     *            double[][]
     * @param sources
     *            double[][]
     * @param highDim
     *            int
     * @param lowDim
     *            int
     */
    private static void copyCenters(double[][] dests, double[][] sources, int highDim, int lowDim) {
        for (int i = 0; i < highDim; i++) {
            for (int j = 0; j < lowDim; j++) {
                dests[i][j] = sources[i][j];
            }
        }
    }

    /**
     * 更新聚类中心坐标
     * 
     * @param k
     *            int 分类个数
     * @param data
     *            kmeans_data
     */
    public static void updateCenters(int k, KmeansData data) {
        double[][] centers = data.centers;
        setDouble2Zero(centers, k, data.dim);//清空centers这个二维数组
        int[] labels = data.labels;
        int[] centerCounts = data.centerCounts;
        for (int i = 0; i < data.dim; i++) {
            for (int j = 0; j < data.length; j++) {
                centers[labels[j]][i] += data.data[j][i];
            }
        }
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < data.dim; j++) {
            	if(centerCounts[i] > 0)
            		centers[i][j] = centers[i][j] / centerCounts[i];/////
            	else {
//            		int rand = (int) (Math.random() * (data.data.length-1));
//            		centers[i][j] = data.data[rand][j]; 
            	}
            }
        }
    }

    /**
     * 计算两点欧氏距离
     * 
     * @param pa
     *            double[]
     * @param pb
     *            double[]
     * @param dim
     *            int 维数
     * @return double 距离
     */
    public static double dist(double[] pa, double[] pb, int dim) {
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
    


    /**
     * 做Kmeans运算
     * 
     * @param k
     *            int 聚类个数
     * @param data
     *            kmeans_data kmeans数据类
     * @param param
     *            kmeans_param kmeans参数类
     * @return kmeans_result kmeans运行信息类
     */
    public static KmeansResult doKmeans(int k, KmeansData data, KmeansParam param) {
        // 预处理
        double[][] centers = new double[k][data.dim]; // 聚类中心点集
        data.centers = centers;
        int[] centerCounts = new int[k]; // 各聚类的包含点个数
        data.centerCounts = centerCounts;
        Arrays.fill(centerCounts, 0);//赋值初始化为0
        int[] labels = new int[data.length]; // 各个点所属聚类标号
        data.labels = labels;
        double[][] oldCenters = new double[k][data.dim]; // 临时缓存旧的聚类中心坐标

        // 初始化聚类中心（随机或者依序选择data内的k个不重复点）
        if (param.initCenterMehtod == KmeansParam.CENTER_RANDOM) { // 随机选取k个初始聚类中心
            Random rn = new Random();
            List<Integer> seeds = new LinkedList<Integer>();
            while (seeds.size() < k) {
                int randomInt = rn.nextInt(data.length);
                if (!seeds.contains(randomInt)) {
                    seeds.add(randomInt);
                }
            }
            Collections.sort(seeds);
            for (int i = 0; i < k; i++) {
                int m = seeds.remove(0);
                for (int j = 0; j < data.dim; j++) {
                    centers[i][j] = data.data[m][j];//初始化中心点
                }
            }
        } else { // 选取前k个点位初始聚类中心
            for (int i = 0; i < k; i++) {
                for (int j = 0; j < data.dim; j++) {
                    centers[i][j] = data.data[i][j];
                }
            }
        }

        // 第一轮迭代
        for (int i = 0; i < data.length; i++) {
            double minDist = dist(data.data[i], centers[0], data.dim);
            int label = 0;
            for (int j = 1; j < k; j++) {
                double tempDist = dist(data.data[i], centers[j], data.dim);
                if (tempDist < minDist) {
                    minDist = tempDist;
                    label = j;
                }
            }
            labels[i] = label;
            centerCounts[label]++;
        }
        updateCenters(k, data);
        copyCenters(oldCenters, centers, k, data.dim);

        // 迭代预处理
        int maxAttempts = param.attempts > 0 ? param.attempts : KmeansParam.MAX_ATTEMPTS;
        int attempts = 1;
        double criteria = param.criteria > 0 ? param.criteria : KmeansParam.MIN_CRITERIA;
        double criteriaBreakCondition = 0;
        boolean[] flags = new boolean[k]; // 标记哪些中心被修改过
        // 迭代
        iterate: while (attempts < maxAttempts) { // 迭代次数不超过最大值，最大中心改变量不超过阈值
            for (int i = 0; i < k; i++) { // 初始化中心点“是否被修改过”标记
                flags[i] = false;
            }
            for (int i = 0; i < data.length; i++) { // 遍历data内所有点
                double minDist = dist(data.data[i], centers[0], data.dim);
                int label = 0;
                for (int j = 1; j < k; j++) {
                    double tempDist = dist(data.data[i], centers[j], data.dim);
                    if (tempDist < minDist) {
                        minDist = tempDist;
                        label = j;
                    }
                }
                if (label != labels[i]) { // 如果当前点被聚类到新的类别则做更新
                    int oldLabel = labels[i];
                    labels[i] = label;
                    centerCounts[oldLabel]--;
                    centerCounts[label]++;
                    flags[oldLabel] = true;
                    flags[label] = true;
                }
            }
            updateCenters(k, data);
            System.err.println("iteration " + attempts);
            attempts++;

            // 计算被修改过的中心点最大修改量是否超过阈值
            double maxDist = 0;
            for (int i = 0; i < k; i++) {
                if (flags[i]) {
                    double tempDist = dist(centers[i], oldCenters[i], data.dim);
                    if (maxDist < tempDist) {
                        maxDist = tempDist;
                    }
                    for (int j = 0; j < data.dim; j++) { // 更新oldCenter
                        oldCenters[i][j] = centers[i][j];
                    }
                }
            }
            System.out.println("maxDist = " + maxDist);
            if (maxDist < criteria) {
            	//System.err.println("iteration " + attempts);
            	//System.out.println("maxDist = " + maxDist);
                criteriaBreakCondition = maxDist;
                break iterate;
            }
        }

        // 输出信息
        KmeansResult rvInfo = new KmeansResult();
        rvInfo.attempts = attempts;
        rvInfo.criteriaBreakCondition = criteriaBreakCondition;
        if (param.isDisplay) {
            System.out.println("k=" + k);
            System.out.println("attempts=" + attempts);
            System.out.println("criteriaBreakCondition=" + criteriaBreakCondition);
            System.out.println("The number of each classes are: ");
            for (int i = 0; i < k; i++) {
                System.out.print(centerCounts[i] + " ");
            }
            System.out.print("\n\n");
        }
        return rvInfo;
    }
    
    public static void main(String str[]) {
    	
    	int numCoords = 2;    /* no. features */
        int numObjs = 30;    /* no. objects */
        int numClusters = 3;  /* no. clusters */
        float threshold = 0.0001f;    /* % objects change membership */
    	double[][] points = new double[numObjs][numCoords];
    	
    	for(int i=0; i<numObjs/3; i++) {
    		for(int j=0; j<numCoords; j++) {
    			points[i][j] = Math.random();
    		}
    	}

    	for(int i=numObjs/3; i<2*numObjs/3; i++) {
    		for(int j=0; j<numCoords; j++) {
    			points[i][j] = Math.random() + 30;
    		}
    	}

    	for(int i=2*numObjs/3; i<numObjs; i++) {
    		for(int j=0; j<numCoords; j++) {
    			points[i][j] = Math.random() + 50;
    		}
    	}
		
		long t1 = System.currentTimeMillis();
		
		KmeansData data = new KmeansData(points, points.length, points[0].length); // 初始化数据结构
		KmeansParam param = new KmeansParam(); // 初始化参数结构
		param.criteria = threshold;
		param.attempts = 1000;
		param.initCenterMehtod = KmeansParam.CENTER_RANDOM; // 设置聚类中心点的初始化模式为随机模式

		// 做kmeans计算，分两类
		Kmeans.doKmeans(numClusters, data, param);

		System.out.println("cost: " + (System.currentTimeMillis()-t1) / 1000.0);
		
		// 查看每个点的所属聚类标号
		System.out.print("The labels of points is: ");
    }
}
