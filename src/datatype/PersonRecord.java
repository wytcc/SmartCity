package datatype;

import java.util.ArrayList;
import java.util.List;

import datatype.Record;

public class PersonRecord {
	public long userId;
	public int recordNumber;
	
	public List<Record> records;
	public PersonRecord(long userId,int recordNumber)
	{
		this.userId=userId;
		this.recordNumber=recordNumber;
		this.records=new ArrayList<Record>();
	}
	public void detectpingpongeffect()
	{//�ƶ�ͨ��ϵͳ�У������һ������������վ�ź�ǿ�Ⱦ��ұ仯���ֻ��ͻ���������վ�������л���������ν�ġ�ƹ��ЧӦ����
		int loopflag=1;
		while(loopflag==1)
		{
			loopflag=0;
			//ȥ
			if(records.size()<3)
				break;
			for(int i=0;i<records.size()-2;i++)
			{
				int site1=records.get(i).site;
				int site2=records.get(i+1).site;
				int site3=records.get(i+2).site;
				long time12=records.get(i+1).time-records.get(i).time;
				long time23=records.get(i+2).time-records.get(i+1).time;
				long psecond=5* 1000; 
				if(site1==site2 && site2==site3)
				{
					records.remove(i+1);
					loopflag=1;
				}
				if(site1==site3 && time12<=psecond&& time23<=psecond)
				{
					records.remove(i+1);
					loopflag=1;
				}
			}
		}
	}
}