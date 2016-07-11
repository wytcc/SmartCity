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
		
		//用数组存某个区间的记录数，timeIntervalCount[0]代表时间区间为0-1min的记录数，timeIntervalCount[1]代表时间区间为1-2min的记录数。
		//int[] timeIntervalCount = new int[300];
		HashMap<Double,Integer> timeIntervalCount = new HashMap<Double,Integer>();
		HashMap<Double,Integer> distanceCount = new HashMap<Double,Integer>();
		
		_records = new ArrayList<Record>();
		//连接MongoDB
		MongoModule mongoModule = new MongoModule();
		mongoModule.mongoDBCollection.deleteMany(new Document());
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		

		File file = new File(path);
		File[] fileList = file.listFiles();//得到path目录下完整路径的文件名，得到的文件名的数组
		
		// basic statistics
		int recordscount = 0;
		int personsCount = 0;
		int line = 1;
		
		long stime = 0;
		long etime = 0;
		try {
			stime= sdf.parse("2014-1-20 00:00:00.00").getTime();//开始时间
			etime = sdf.parse("2014-12-24 00:00:00.00").getTime();//结束时间
		}catch(ParseException e){
			e.printStackTrace();
		}
		
		BufferedReader reader = null;
		
		try {
			System.out.println("开始读取原始轨迹数据，写成二进制文件 以行为单位读取文件内容，一次读一整行：");
			for (int fileIndex = 0; fileIndex < fileList.length; fileIndex++) {
				reader = new BufferedReader(new FileReader(fileList[fileIndex]));//读这个文件
				String tempString = null;
				while ((tempString = reader.readLine()) != null) {
					// 每10000行显示一次行号
					//System.out.println("here");
					if (line % 10000 == 0)
						System.out.println("已经读取到第" + line + "行");
					line++;
//					if(line == 10000) break;
					String[] str = tempString.split(" ");//使用空格切割
//					System.out.println(tempString);
					
					try {
						//读入一行数据，切割处理存入Record数据结构中
						long userId = Long.parseLong(str[0]);
						int recordNumber = (str.length - 1) / 4;//记录的数目，剪掉的1是userid，每一字数据在用空格分割后是有四段的，所以是除以4
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
							pr.records.add(record);//personrecord的构造函数中只是指定了用户id和记录数，实际上personrecord的属性中还有一个records这个arrayList，添加有效的记录，在这里添加
						}
						pr.recordNumber = validRecordNumber;//这里更新personrecord的记录数为有效的记录数
						//pr.recordNumber = pr.records.size();//和上面一个意思
						
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
						//标记停留点
						//停留时间30分钟，距离500m
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
						//统计，计算出相邻记录的时间间隔，步长为1min，如果相邻记录在同一个地点则时间累加，统计出在0-1min、1-2min......区间中记录的个数
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
								//如果存在key，则取出value，让value++，否则，value值设为1
								if(timeIntervalCount.containsKey(timeFloor)){
									int currentCount = timeIntervalCount.get(timeFloor);
									currentCount++;
									timeIntervalCount.put(timeFloor, currentCount);
								}else{
									timeIntervalCount.put(timeFloor, 1);
								}
								time = 0;
							}
							
							//处理距离相关，为了取巧，先让距离乘以10，然后后面的处理逻辑与时间的相同，存的时候区间再还原回来
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
//						//统计，计算出相邻记录的时间和距离，写入csv文件 （第一次统计记录）
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

						
						int flagfeature=1;//1 有效 在鹿城区
						for(int i=0;i<pr.records.size();i++){
							Station sta = sp.getStation(pr.records.get(i).lac, pr.records.get(i).cell);
							if(sta.longitude<120.53695 || sta.longitude>120.900696)
								flagfeature=0;
							if(sta.latitude<27.846675  || sta.latitude>28.162664)
								flagfeature=0;
						}
						if(flagfeature==0){
							continue;//如果无效就跳过这一行记录
						}
						
							
						// PersonList.add(pr);
						personsCount++;//读完一行数据后，即读完一个人的数据，人数加一，也可以表示当前人的标号，记录数加上这个人的记录数
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
			System.out.println("Rawdata二进制文件写入完毕");
			
		}catch(IOException e){
			e.printStackTrace();
		}finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}//try while 读文件
		
		//-------------------------------
		//统计，计算出相邻记录的时间和距离，写入csv文件  （用于存储生成统计分析数据到csv文件）
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
				//统计，计算出相邻记录的时间和距离，写入csv文件  （用于存储生成统计分析数据到csv文件）
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
		for (int i = 0; i < validRecordNumber; i++) {//去除掉isFilteredByTime为true的记录
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
			if(interval>StaticParam.TIME_INTERVAL_INSERT){//两个小时
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
			new_record_insert.add(pr.records.get(startid));//加进去最后一个
		}
		validRecordNumber=new_record_insert.size();
		pr.records=new_record_insert;//更新为差值以后的记录
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
			Station station1 = sp.getStation(pr.records.get(startIdx).lac, pr.records.get(startIdx).cell);//利用id得到基站station的经纬度
			Station station2 = sp.getStation(pr.records.get(endIdx).lac, pr.records.get(endIdx).cell);
			double dis = GPS2Dist.distance(station1.latitude, station1.longitude, station2.latitude,
					station2.longitude);
			double time = (pr.records.get(endIdx).time - pr.records.get(startIdx).time) / 1000.0 / 60.0
					/ 60.0;//得到以小时为单位的时间
			double spd = dis / time;//speed 速度=路程/时间

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
		System.out.println("程序运行结束");
	}
}
