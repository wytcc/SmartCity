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
     * double[][] Ԫ��ȫ��0
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
     * ����Դ��ά����Ԫ�ص�Ŀ���ά���� foreach (dests[highDim][lowDim] = sources[highDim][lowDim]);
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
     * ���¾�����������
     * 
     * @param k
     *            int �������
     * @param data
     *            kmeans_data
     */
    public static void updateCenters(int k, KmeansData data) {
        double[][] centers = data.centers;
        setDouble2Zero(centers, k, data.dim);//���centers�����ά����
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
     * ��������ŷ�Ͼ���
     * 
     * @param pa
     *            double[]
     * @param pb
     *            double[]
     * @param dim
     *            int ά��
     * @return double ����
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
     * ��Kmeans����
     * 
     * @param k
     *            int �������
     * @param data
     *            kmeans_data kmeans������
     * @param param
     *            kmeans_param kmeans������
     * @return kmeans_result kmeans������Ϣ��
     */
    public static KmeansResult doKmeans(int k, KmeansData data, KmeansParam param) {
        // Ԥ����
        double[][] centers = new double[k][data.dim]; // �������ĵ㼯
        data.centers = centers;
        int[] centerCounts = new int[k]; // ������İ��������
        data.centerCounts = centerCounts;
        Arrays.fill(centerCounts, 0);//��ֵ��ʼ��Ϊ0
        int[] labels = new int[data.length]; // ����������������
        data.labels = labels;
        double[][] oldCenters = new double[k][data.dim]; // ��ʱ����ɵľ�����������

        // ��ʼ���������ģ������������ѡ��data�ڵ�k�����ظ��㣩
        if (param.initCenterMehtod == KmeansParam.CENTER_RANDOM) { // ���ѡȡk����ʼ��������
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
                    centers[i][j] = data.data[m][j];//��ʼ�����ĵ�
                }
            }
        } else { // ѡȡǰk����λ��ʼ��������
            for (int i = 0; i < k; i++) {
                for (int j = 0; j < data.dim; j++) {
                    centers[i][j] = data.data[i][j];
                }
            }
        }

        // ��һ�ֵ���
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

        // ����Ԥ����
        int maxAttempts = param.attempts > 0 ? param.attempts : KmeansParam.MAX_ATTEMPTS;
        int attempts = 1;
        double criteria = param.criteria > 0 ? param.criteria : KmeansParam.MIN_CRITERIA;
        double criteriaBreakCondition = 0;
        boolean[] flags = new boolean[k]; // �����Щ���ı��޸Ĺ�
        // ����
        iterate: while (attempts < maxAttempts) { // �����������������ֵ��������ĸı�����������ֵ
            for (int i = 0; i < k; i++) { // ��ʼ�����ĵ㡰�Ƿ��޸Ĺ������
                flags[i] = false;
            }
            for (int i = 0; i < data.length; i++) { // ����data�����е�
                double minDist = dist(data.data[i], centers[0], data.dim);
                int label = 0;
                for (int j = 1; j < k; j++) {
                    double tempDist = dist(data.data[i], centers[j], data.dim);
                    if (tempDist < minDist) {
                        minDist = tempDist;
                        label = j;
                    }
                }
                if (label != labels[i]) { // �����ǰ�㱻���ൽ�µ������������
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

            // ���㱻�޸Ĺ������ĵ�����޸����Ƿ񳬹���ֵ
            double maxDist = 0;
            for (int i = 0; i < k; i++) {
                if (flags[i]) {
                    double tempDist = dist(centers[i], oldCenters[i], data.dim);
                    if (maxDist < tempDist) {
                        maxDist = tempDist;
                    }
                    for (int j = 0; j < data.dim; j++) { // ����oldCenter
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

        // �����Ϣ
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
		
		KmeansData data = new KmeansData(points, points.length, points[0].length); // ��ʼ�����ݽṹ
		KmeansParam param = new KmeansParam(); // ��ʼ�������ṹ
		param.criteria = threshold;
		param.attempts = 1000;
		param.initCenterMehtod = KmeansParam.CENTER_RANDOM; // ���þ������ĵ�ĳ�ʼ��ģʽΪ���ģʽ

		// ��kmeans���㣬������
		Kmeans.doKmeans(numClusters, data, param);

		System.out.println("cost: " + (System.currentTimeMillis()-t1) / 1000.0);
		
		// �鿴ÿ���������������
		System.out.print("The labels of points is: ");
    }
}
