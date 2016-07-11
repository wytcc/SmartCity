package preconditioning;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import datatype.KmeansData;
import datatype.PersonRecord;
import datatype.Record;
import datatype.Station;
import util.GPS2Dist;
import util.MongoModule;
import util.StaticParam;
import util.StationParser;


public class Preconditioning {

	public double[][] points;
	public List<Record> _records;
	public KmeansData data;
	public StationParser sp = new StationParser();
	
	public void readFileByLine(String path){
		
		try{
			FileWriter fw = new FileWriter("statistics-time.csv");
			String header = "time-start,time-end,count\r\n";
			fw.write(header);
			fw.flush();
			fw.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		
		try{
			FileWriter fw = new FileWriter("statistics-distance.csv");
			String header = "dis-start,dis-end,count\r\n";
			fw.write(header);
			fw.flush();
			fw.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		
		//�������ĳ������ļ�¼����timeIntervalCount[0]����ʱ������Ϊ0-1min�ļ�¼����timeIntervalCount[1]����ʱ������Ϊ1-2min�ļ�¼����
		//int[] timeIntervalCount = new int[300];
		HashMap<Double,Integer> timeIntervalCount = new HashMap<Double,Integer>();
		HashMap<Double,Integer> distanceCount = new HashMap<Double,Integer>();
		
		_records = new ArrayList<Record>();
		//����MongoDB
		MongoModule mongoModule = new MongoModule();
		mongoModule.mongoDBCollection.deleteMany(new Document());
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		

		File file = new File(path);
		File[] fileList = file.listFiles();//�õ�pathĿ¼������·�����ļ������õ����ļ���������
		
		// basic statistics
		int recordscount = 0;
		int personsCount = 0;
		int line = 1;
		
		long stime = 0;
		long etime = 0;
		try {
			stime= sdf.parse("2014-1-20 00:00:00.00").getTime();//��ʼʱ��
			etime = sdf.parse("2014-12-24 00:00:00.00").getTime();//����ʱ��
		}catch(ParseException e){
			e.printStackTrace();
		}
		
		BufferedReader reader = null;
		
		try {
			System.out.println("��ʼ��ȡԭʼ�켣���ݣ�д�ɶ������ļ� ����Ϊ��λ��ȡ�ļ����ݣ�һ�ζ�һ���У�");
			for (int fileIndex = 0; fileIndex < fileList.length; fileIndex++) {
				reader = new BufferedReader(new FileReader(fileList[fileIndex]));//������ļ�
				String tempString = null;
				while ((tempString = reader.readLine()) != null) {
					// ÿ10000����ʾһ���к�
					//System.out.println("here");
					if (line % 10000 == 0)
						System.out.println("�Ѿ���ȡ����" + line + "��");
					line++;
//					if(line == 10000) break;
					String[] str = tempString.split(" ");//ʹ�ÿո��и�
//					System.out.println(tempString);
					
					try {
						//����һ�����ݣ��и�����Record���ݽṹ��
						long userId = Long.parseLong(str[0]);
						int recordNumber = (str.length - 1) / 4;//��¼����Ŀ��������1��userid��ÿһ���������ÿո�ָ�������Ķεģ������ǳ���4
						if (recordNumber < 5)
							continue;
						int validRecordNumber = 0;
						PersonRecord pr = new PersonRecord(userId, recordNumber);
						
						for (int i = 0; i < recordNumber; i++) {
							Record record = new Record();
							record.time = sdf.parse(str[i * 4 + 1] + " " + str[i * 4 + 2] + "0").getTime();
							if (str[i * 4 + 3].equals(""))
								continue;
							record.lac = Integer.parseInt(str[i * 4 + 3]);
							if (str[i * 4 + 4].equals(""))
								continue;
							record.cell = Integer.parseInt(str[i * 4 + 4]);

							Station station = sp.getStation(record.lac, record.cell);
							if (station == null || station.site_map == -1)
								continue;
							else
								record.site = station.site_map;//site_index
							validRecordNumber++;
							pr.records.add(record);//personrecord�Ĺ��캯����ֻ��ָ�����û�id�ͼ�¼����ʵ����personrecord�������л���һ��records���arrayList�������Ч�ļ�¼�����������
						}
						pr.recordNumber = validRecordNumber;//�������personrecord�ļ�¼��Ϊ��Ч�ļ�¼��
						//pr.recordNumber = pr.records.size();//������һ����˼
						
						// re-sort all records according to time
						// ----------------------------------------------------------------------------------------
						Collections.sort(pr.records, new Comparator<Record>() {
							public int compare(Record arg0, Record arg1) {
								return new Long(arg0.time).compareTo(new Long(arg1.time));
							}
						});
						
						//System.out.println("here");
						
						//delete record1.time = record2.time
						pr = deleteRecordWithSameTime(validRecordNumber, pr);
						validRecordNumber = pr.recordNumber;
						
						// test if all records resort right
						for (int i = 0; i < validRecordNumber - 1; i++) {
							long time1 = pr.records.get(i).time;
							long time2 = pr.records.get(i + 1).time;
							if (time1 >= time2) {
								System.out.println("time1>=time2");
							}
						}
						
						//insert record
						pr = insertRecord(validRecordNumber, pr);
						validRecordNumber = pr.recordNumber;
						
						//filt those unusual records
						pr = filtUnusualRecords(validRecordNumber, pr);
						validRecordNumber = pr.recordNumber;
						
						//System.out.println("here");
						
						
						
						//------------------------------------------------------------------
						//���ͣ����
						//ͣ��ʱ��30���ӣ�����500m
						int[] stay = new int[validRecordNumber];
						
						for (int i = 0; i < validRecordNumber; i++) {
							stay[i] = 0;
							Station staI = sp.getStation(pr.records.get(i).lac, pr.records.get(i).cell);
							int j = i;
							for (; j < validRecordNumber; j++) {
								Station staJ = sp.getStation(pr.records.get(j).lac, pr.records.get(j).cell);
								double dis = GPS2Dist.distance(staI.latitude, staI.longitude, staJ.latitude,
										staJ.longitude);
								if (dis > StaticParam.DISTANCE_STAY) {
									break;
								}
							}
							if (j > i + 1 && j < validRecordNumber
									&& (pr.records.get(j).time - pr.records.get(i).time) > StaticParam.TIME_STAY) {
								for (int k = i; k < j; k++) {
									stay[k] = 1;
								}
								stay[j - 1] = 2;
								i = j;
							}
							if (j == validRecordNumber) {
								for (int k = i; k < j; k++) {
									stay[k] = 1;
								}
								stay[j-1] = 2;
							}
						}
						for (int i = 0; i < validRecordNumber-1;i++) {
							if(stay[i]==0 && stay[i+1]==2){
								stay[i+1]=0;
							}
							else if(stay[i]==1 && stay[i+1]==0){
								stay[i]=0;
								if(i>=1)
									i=i-2;
							}
							else if(stay[i]==0 && stay[i+1]==2){
								stay[i+1]=0;
							}
							else if(stay[i]==2 && stay[i+1]==2){
								stay[i+1]=0;
							}
						}

						for (int i = 0; i < validRecordNumber-1;i++) {
							if(stay[i]==1 && stay[i+1]==1){
								
							}else if(stay[i]==0 && stay[i+1]==1){}
							else if(stay[i]==1 && stay[i+1]==2){}
							else if(stay[i]==0 && stay[i+1]==0){}
							else if(stay[i]==2 && stay[i+1]==0){}
							else if(stay[i]==2 && stay[i+1]==1){}
							else{
							System.out.println(i+" "+stay[i]+stay[i+1]+"error");
							}
						}

						//System.out.println("here");
						
						//--------------------------------------------------------------------------------------
						//ͳ�ƣ���������ڼ�¼��ʱ����������Ϊ1min��������ڼ�¼��ͬһ���ص���ʱ���ۼӣ�ͳ�Ƴ���0-1min��1-2min......�����м�¼�ĸ���
						double[] speed = new double[validRecordNumber];
						double[] distanceToNext = new double[validRecordNumber];
						double[] timeToNext = new double[validRecordNumber];
						double time = 0;
						for (int i = 0; i < validRecordNumber - 1; i++) {
							Station sta = sp.getStation(pr.records.get(i).lac, pr.records.get(i).cell);
							Station sta2 = sp.getStation(pr.records.get(i + 1).lac, pr.records.get(i + 1).cell);
							double dis = GPS2Dist.distance(sta.latitude, sta.longitude, sta2.latitude, sta2.longitude);
							time += (pr.records.get(i + 1).time - pr.records.get(i).time) / 1000.0 / 60.0;
							if(dis != 0.0){
								double timeFloor = Math.floor(time);
								//�������key����ȡ��value����value++������valueֵ��Ϊ1
								if(timeIntervalCount.containsKey(timeFloor)){
									int currentCount = timeIntervalCount.get(timeFloor);
									currentCount++;
									timeIntervalCount.put(timeFloor, currentCount);
								}else{
									timeIntervalCount.put(timeFloor, 1);
								}
								time = 0;
							}
							
							//���������أ�Ϊ��ȡ�ɣ����þ������10��Ȼ�����Ĵ����߼���ʱ�����ͬ�����ʱ�������ٻ�ԭ����
							double tempDis = dis * 10;
							double tempDisFloor = Math.floor(tempDis);
							if(distanceCount.containsKey(tempDisFloor)){
								int currentCount = distanceCount.get(tempDisFloor);
								currentCount++;
								distanceCount.put(tempDisFloor, currentCount);
							}else {
								distanceCount.put(tempDisFloor, 1);
							}
						}
						//----------------------------------------------------------------------------------------------
						
						
//						//-------------------------------
//						//ͳ�ƣ���������ڼ�¼��ʱ��;��룬д��csv�ļ� ����һ��ͳ�Ƽ�¼��
//						try{
//							FileWriter fw = new FileWriter("statistics.csv", true);
//							
//							StringBuffer s = new StringBuffer();
//							for (int i = 0; i < validRecordNumber-1; i++){
//								s.append(timeToNext[i]+","+distanceToNext[i]+"\r\n");
//							}
//							fw.write(s.toString());
//							fw.flush();
//							fw.close();
//						}catch(IOException e){
//							e.printStackTrace();
//						}
//						
//						//---------------------------------------

						
						int flagfeature=1;//1 ��Ч ��¹����
						for(int i=0;i<pr.records.size();i++){
							Station sta = sp.getStation(pr.records.get(i).lac, pr.records.get(i).cell);
							if(sta.longitude<120.53695 || sta.longitude>120.900696)
								flagfeature=0;
							if(sta.latitude<27.846675  || sta.latitude>28.162664)
								flagfeature=0;
						}
						if(flagfeature==0){
							continue;//�����Ч��������һ�м�¼
						}
						
							
						// PersonList.add(pr);
						personsCount++;//����һ�����ݺ󣬼�����һ���˵����ݣ�������һ��Ҳ���Ա�ʾ��ǰ�˵ı�ţ���¼����������˵ļ�¼��
						recordscount += pr.recordNumber;
							
						// mongodb and java
						Document objectDocument = new Document();
						objectDocument.append("userid", pr.userId);
						objectDocument.append("index", personsCount);
						ArrayList recordList = new ArrayList();
						for (int i = 0; i < pr.recordNumber; i++) {
							ArrayList listTemp = new ArrayList();
							Station sta = sp.getStation(pr.records.get(i).lac, pr.records.get(i).cell);
							listTemp.add(sta.latitude);
							listTemp.add(sta.longitude);

							listTemp.add(pr.records.get(i).time);
							listTemp.add(stay[i]);
							listTemp.add(sta.site_map);
							recordList.add(listTemp);
						}
						objectDocument.append("records",
								new Document().append("type", "LineString").append("coordinates", recordList));
						mongoModule.mongoDBCollection.insertOne(objectDocument);
							
						//_records
						for (int i = 0; i < pr.recordNumber-1; i++) {
							if((stay[i]==1 &&stay[i+1]==1)||(stay[i]==1 &&stay[i+1]==2)){
								Record recordTmp = pr.records.get(i);
								_records.add(recordTmp);
							}
						}						

						
					}catch(ParseException e){
						e.printStackTrace();
					}
				}
			}
			reader.close();

			System.out.println("Lines:" + line);
			System.out.println("PersonsCount:" + personsCount);
			System.out.println("recordscount" + recordscount);
			System.out.println("Rawdata�������ļ�д�����");
			
		}catch(IOException e){
			e.printStackTrace();
		}finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}//try while ���ļ�
		
		//-------------------------------
		//ͳ�ƣ���������ڼ�¼��ʱ��;��룬д��csv�ļ�  �����ڴ洢����ͳ�Ʒ������ݵ�csv�ļ���
		try{
			FileWriter fw = new FileWriter("statistics-time.csv", true);
			
			StringBuffer s = new StringBuffer();

			Iterator iter = timeIntervalCount.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry entry = (Map.Entry) iter.next();
				double key = (double)entry.getKey();
				int val = (int)entry.getValue();
				s.append(key+","+(key+1)+","+val+"\r\n");
			}
			
			fw.write(s.toString());
			fw.flush();
			fw.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		
		//---------------------------------------
		
		//-------------------------------
				//ͳ�ƣ���������ڼ�¼��ʱ��;��룬д��csv�ļ�  �����ڴ洢����ͳ�Ʒ������ݵ�csv�ļ���
				try{
					FileWriter fw = new FileWriter("statistics-distance.csv", true);
					
					StringBuffer s = new StringBuffer();

					Iterator iter = distanceCount.entrySet().iterator();
					while (iter.hasNext()) {
						Map.Entry entry = (Map.Entry) iter.next();
						double key = (double)entry.getKey();
						int val = (int)entry.getValue();
						s.append(key/10+","+(key+1)/10+","+val+"\r\n");
					}
					
					fw.write(s.toString());
					fw.flush();
					fw.close();
				}catch(IOException e){
					e.printStackTrace();
				}
				
				//---------------------------------------
	}
	
	public PersonRecord deleteRecordWithSameTime(int validRecordNumber, PersonRecord pr){
		//time=time---------------------------------------------
		//delete record1.time = record2.time
		boolean isFilteredByTime[] = new boolean[validRecordNumber];
		for (int i = 0; i < isFilteredByTime.length; i++) {
			isFilteredByTime[i] = false;// set i as filtered
		}
		int startTimeIndex = 0;
		while (startTimeIndex < validRecordNumber - 1) {
			int endTimeIdx = startTimeIndex + 1;
			while (endTimeIdx < validRecordNumber
					&& pr.records.get(endTimeIdx).time == pr.records.get(startTimeIndex).time){
				isFilteredByTime[endTimeIdx] = true;// discard endIdx item
				endTimeIdx++;
			}
			startTimeIndex = endTimeIdx;
		}
		ArrayList<Record> newRecord = new ArrayList<Record>();
		for (int i = 0; i < validRecordNumber; i++) {//ȥ����isFilteredByTimeΪtrue�ļ�¼
			if (!isFilteredByTime[i]) {
				newRecord.add(pr.records.get(i));
			}
		}
		// update valid record number
		validRecordNumber = newRecord.size();
		pr.recordNumber = validRecordNumber;
		// update pr.records
		pr.records = newRecord;
		return pr;
	}
	
	public PersonRecord insertRecord(int validRecordNumber, PersonRecord pr){
		
		int startid=0;
		ArrayList<Record> new_record_insert = new ArrayList<Record>();
		
		while(startid<validRecordNumber-1){
			long starttime=pr.records.get(startid).time;
			long endtime=pr.records.get(startid+1).time;
			long interval=endtime-starttime;
			new_record_insert.add(pr.records.get(startid));
			if(interval>StaticParam.TIME_INTERVAL_INSERT){//����Сʱ
				while(interval>StaticParam.TIME_INTERVAL_INSERT){
					interval=interval/2;
				}
				for(long i=interval+starttime;i<endtime;i=i+interval){
					Record s=new Record();
					s.cell=pr.records.get(startid).cell;
					s.lac=pr.records.get(startid).lac;
					s.site=pr.records.get(startid).site;
					s.time=i;
					new_record_insert.add(s);
				}
			}
			startid++;
		}
		if(validRecordNumber>1){
			new_record_insert.add(pr.records.get(startid));//�ӽ�ȥ���һ��
		}
		validRecordNumber=new_record_insert.size();
		pr.records=new_record_insert;//����Ϊ��ֵ�Ժ�ļ�¼
		pr.recordNumber=validRecordNumber;
		
		return pr;
	}
	
	public PersonRecord filtUnusualRecords(int validRecordNumber, PersonRecord pr){
		// Here we filter those unusual
		// records---------------------------------------------------------------------
		boolean isFiltered[] = new boolean[validRecordNumber];
		for (int i = 0; i < isFiltered.length; i++) {
			isFiltered[i] = false;// set i as filtered
		}

		int startIdx = 0;
		// Here we use sweep-line algorithm, only scan the sequence
		// once to choose out all unusual items---------
		while (startIdx < validRecordNumber - 1) {
			int endIdx = startIdx + 1;
			Station station1 = sp.getStation(pr.records.get(startIdx).lac, pr.records.get(startIdx).cell);//����id�õ���վstation�ľ�γ��
			Station station2 = sp.getStation(pr.records.get(endIdx).lac, pr.records.get(endIdx).cell);
			double dis = GPS2Dist.distance(station1.latitude, station1.longitude, station2.latitude,
					station2.longitude);
			double time = (pr.records.get(endIdx).time - pr.records.get(startIdx).time) / 1000.0 / 60.0
					/ 60.0;//�õ���СʱΪ��λ��ʱ��
			double spd = dis / time;//speed �ٶ�=·��/ʱ��

			while (endIdx < validRecordNumber
					&& pr.records.get(endIdx).time - pr.records.get(startIdx).time < StaticParam.TIME_INTERVAL_UNUSUAL && time > 0
					&& dis > 0 && spd > StaticParam.SPEED) {
				isFiltered[endIdx] = true;// discard endIdx item
				endIdx++;
				if (endIdx < validRecordNumber) {
					station1 = sp.getStation(pr.records.get(startIdx).lac, pr.records.get(startIdx).cell);
					station2 = sp.getStation(pr.records.get(endIdx).lac, pr.records.get(endIdx).cell);
					dis = GPS2Dist.distance(station1.latitude, station1.longitude, station2.latitude,
							station2.longitude);
					time = (pr.records.get(endIdx).time - pr.records.get(startIdx).time) / 1000.0 / 60.0
							/ 60.0;
					spd = dis / time;
				}
			}
			startIdx = endIdx;
		}
		ArrayList<Record> new_record = new ArrayList<Record>();
		for (int i = 0; i < validRecordNumber; i++) {
			if (!isFiltered[i]) {
				new_record.add(pr.records.get(i));
			}
		}
        validRecordNumber = new_record.size();
        
		// update valid record number
		pr.recordNumber = validRecordNumber;
		// update pr.records
		pr.records = new_record;
		return pr;
	}
	
	
	
	public static void main(String[] args) {
		Preconditioning program = new Preconditioning();
		program.readFileByLine("data/raw");
		System.out.println("�������н���");
	}
}
