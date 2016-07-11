package datatype;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 *
 * @param <b>data</b> <i>in double[length][dim]</i><br/>length��instance�����꣬��i(0~length-1)��instanceΪdata[i]
 * @param <b>length</b> <i>in</i> instance����
 * @param <b>dim</b> <i>in</i> instanceά��
 * @param <b>labels</b> <i>out int[length]</i><br/>�����instance�����ľ�����(0~k-1)
 * @param <b>centers</b> <i>in out double[k][dim]</i><br/>k���������ĵ�����꣬��i(0~k-1)�����ĵ�Ϊcenters[i]
 * @author Yuanbo She
 *
 */
public class KmeansData {
	public double[][] data;
	public int length;
	public int dim;
	public int[] labels;
	public double[][] centers;
	public int[] centerCounts;//������İ��������
	
	public KmeansData(double[][] data, int length, int dim) {
		this.data = data;
		this.length = length;
		this.dim = dim;
	}
	
	private class unit {//�Զ���һ������
		public int index;//���
		public int counts;//����

		public unit(int index, int counts) {
			this.index = index;
			this.counts = counts;
		}
	}
	
	public void sort() {
		ArrayList<unit> temp = new ArrayList<unit>(centerCounts.length);
		for(int j=0; j<centerCounts.length; j++)
			temp.add(new unit(j, centerCounts[j]));
		
		Collections.sort(temp, new Comparator<unit>() {

			@Override
			public int compare(unit o1, unit o2) {
				
				if(o1.counts > o2.counts)
					return -1;
				else if(o1.counts <= o2.counts) {
					return 1;
				} else
					return 0;

			}

		});
		
		int[] centerCounts_temp = new int[centerCounts.length];
		double[][] centers_temp = new double[centers.length][centers[0].length];
		int[] sorted_map = new int[centerCounts.length];
		for(int i=0; i<centerCounts_temp.length; i++) {
			centerCounts_temp[i] = centerCounts[temp.get(i).index];
			centers_temp[i] = centers[temp.get(i).index].clone();
			sorted_map[temp.get(i).index] = i;
		}
		
		int[] labels_temp = new int[labels.length];
		for(int i=0; i<labels_temp.length; i++) {
			labels_temp[i] = sorted_map[labels[i]];
		}
		
		centerCounts = centerCounts_temp;
		centers = centers_temp;
		labels = labels_temp;
		
		//System.out.println();
	}
}