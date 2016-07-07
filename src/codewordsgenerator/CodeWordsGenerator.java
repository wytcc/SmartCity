package codewordsgenerator;

import java.awt.Color;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryOperators;
import com.mysql.jdbc.PreparedStatement;

import datatype.CellSite;
import datatype.Element;
import datatype.Meta;
import datatype.SQLInsert;
import util.ClusterModule;
import util.GeoInfo;
import util.Kmeans;
import util.MobilityEntropy;
import util.MongoModule;
import util.ResidenceDistribution;
import util.SQLModule;
import util.StationParser;


class SortByDis implements Comparator {
	public int compare(Object o1, Object o2) {
	
		SQLInsert s1 = (SQLInsert) o1;
		SQLInsert s2 = (SQLInsert) o2;
		if(s1.distance==s2.distance)
			return 0;
		if(s1.distance>=0&&s2.distance>=0){
			if (s1.distance>s2.distance)
				return 1;
			else
				return -1;
		}
		else {
			return -1;
		}
	}
}

public class CodeWordsGenerator {
	StationParser sp = new StationParser();
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	long data_starttime, data_endtime;

	long stime = 0;
	long etime = 0;
	long interval = 0;
	long frameinterval = 0;
	MongoModule module = new MongoModule();
	Color[] col_schema;
	int catalogs=10;

	public enum fType {
		ETrand, Eunc,AveSpeed, MoveDis, Rg, MoveRatio, AveLocX, AveLocY, HomeLocX, HomeLocY// ,
																				// HomeDis

	};


	public Element[] codewords;
	public int validTrajCount= 0;
	public int validFrameCount = 0;
	public int nearc = 3;
	ClusterModule cm;
	ResidenceDistribution rd;
	GeoInfo gi;
	String databasename="mobiledata";//SQL数据库名
	int size=50;
	public CodeWordsGenerator() {

		
		if(databasename=="mobiledata_test"){
			System.out.println("Use test mongodb");
			module.coll2=module.db.getCollection("templist_test");
			module.coll3=module.db.getCollection("featurevector_test");
		}
		else{
			module.coll2=module.db.getCollection("templist");
			module.coll3=module.db.getCollection("featurevectorw");
		}
		if(module.coll2.find().count()>0){  
			module.coll2.remove(new BasicDBObject());  //To remove all documents use the BasicDBObject
	     }
		if(module.coll3.find().count()>0){  
			module.coll3.remove(new BasicDBObject());  
	    }
		System.out.println("mongodb clear");
		module.coll2.dropIndexes();//删除当前集合中的所有索引
		module.coll3.dropIndexes();
		cm = new ClusterModule();
		
		rd = new ResidenceDistribution(sp);
		gi = new GeoInfo(sp);

		try {
			stime = sdf.parse("2014-1-21 00:00:00.00").getTime();
			etime = sdf.parse("2014-1-28 00:00:00.00").getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}

		interval = 2 * 60 * 60 * 1000;// 2 hour
		frameinterval = 15 * 60 * 1000;// 15 min
		
		userTrajReader();

		cm.cluster(codewords, validTrajCount, validFrameCount);
		savefeaturevector();

	}

	public void savefeaturevector(){
		System.out.println("save feature vector");
		DBCursor cursor=module.coll2.find();//得到所有的
		while(cursor.hasNext()){
			BasicDBObject dbj=(BasicDBObject) cursor.next();
			int validFrameCount=(int) dbj.get("validFrameCount");
			ArrayList featurevector=new ArrayList();
			for(int i=0;i<fType.values().length;i++){
				featurevector.add(cm.data.data[validFrameCount][i]);
			}
			dbj.append("featurevector",featurevector);
			module.coll3.insert(new BasicDBObject("validFrameCount",validFrameCount), dbj);
		}
		module.coll3.createIndex(new BasicDBObject("starttime",1));
		module.coll3.createIndex(new BasicDBObject("endtime",-1));
		module.coll3.createIndex(new BasicDBObject("HomeLocX",1));
		module.coll3.createIndex(new BasicDBObject("HomeLocY",1));
	}

	public void userTrajReader() {
		int streamNum;//表示轨迹的段数，动的段数加静的段数
		try {

			DataOutputStream datawriter = new DataOutputStream(
					new BufferedOutputStream(new FileOutputStream("data/TrajectoryData")));

			int start = 0;
			System.out.println("**** " + size);

			ArrayList<Element> element = new ArrayList<Element>();
//			ArrayList<Element> selement = new ArrayList<Element>();
//			ArrayList<Element> melement = new ArrayList<Element>();
			DBCursor cursor=module.coll.find();
			for (int i = 0; i < size; i++) {
				DBObject dbj=cursor.next();
				long userid=(long) dbj.get("userid");
				
				DBObject records=(DBObject) dbj.get("records");
				ArrayList recordsList=(ArrayList) records.get("coordinates");
				int rec_size=recordsList.size();//coordinate数
				
				
				Meta[] p = new Meta[rec_size];//p是一个userid的，即一个人，的所有coordinate

				ArrayList<Meta> pp = new ArrayList<Meta>(p.length);
				int prestate=-1;
				streamNum=0;
				

				for (int j = 0; j < rec_size; j++) {//遍历存着coordinate的那个ArrayList

					ArrayList info = (ArrayList) recordsList.get(j);
					long time=(long) info.get(2);
					Double longitude=(Double)info.get(1);
					Double latitude=(Double)info.get(0);
					int state=(int) info.get(3);
					int site=(int) info.get(4);
					
					int cluNo = -1;
					
					p[j] = new Meta(time, userid, state, longitude, latitude, cluNo);
					p[j].site = site;//因为构造函数中没有他
					
//					if(prestate==-1){//prestate为-1代表没有初始化,prestate代表的意义是前一个状态
//						prestate=state;//赋值为从数据库中读出的state的值
//						streamnum++;
//					}else{
//						if(state==1){
//							if(prestate==0){
//								streamnum++;
//								prestate=1;
//							}
//							else if(prestate==1){
//								;
//							}
//							else{
//								streamnum++;
//								prestate=1;
//							}
//						}else if(state==2){
//							if(prestate==0||prestate==2){
//								prestate=2;
//								System.err.println("Error1 "+i);
//								break;
//							}
//							else{
//								prestate=2;
//							}
//						}else {
//							if(prestate==0){;}
//							else if(prestate==2){
//								prestate=0;
//								streamnum++;
//							}else {
//								prestate=state;
//								System.err.println("Error2 "+i);
//							}
//						}
//					}
					
					if (prestate == -1){//prestate代表前一个记录的停留标记
						prestate = state;
						streamNum++;
					}else if (prestate == 0){
						//如果现在的标记是1，则段数加一
						if (state == 1){
							streamNum++;
							prestate = state;
						}
						//如果现在的标记仍然是0，则不作任何处理
					}else if (prestate == 1){
						//如果现在的标记是0，则段数加一
						if (state == 0){
							streamNum++;
							prestate = state;
						}
						//如果现在的标记仍然是1，则不作任何处理
					}
					
				}

				//////////////////////////////////////////

				if (i >= start && i < start + size) {

					if (isValid(p)) {
						Element e = new Element();
						e.id = userid;
						e.moveornot=new boolean[streamNum];
						e.time= new Long[streamNum][2];//猜测，这里的二维分别代表开始时间和结束时间
						
						e.featureVector = new float[streamNum][fType.values().length];//特征向量，分别描述每一段是什么
						e.location = new float[streamNum][(int) (2 * interval / frameinterval)];
						float Residence_longitude = 0;
						float Residence_latitude = 0;
						//////////////
						float[][] ret = rd.residenceJudge(p.clone());

						if (ret != null) {
							for (int ret_i = 0; ret_i < ret[0].length; ret_i++) {
								CellSite a = sp.site.get((int) ret[0][ret_i]);
								Residence_longitude += sp.site.get((int) ret[0][ret_i]).longitude * ret[1][ret_i];
								Residence_latitude += sp.site.get((int) ret[0][ret_i]).latitude * ret[1][ret_i];
							}
							e.residence = new int[ret[0].length];
							e.residenceRatio = new float[ret[1].length];
							for (int r = 0; r < ret[0].length; r++) {
								e.residence[r] = (int) ret[0][r];
								e.residenceRatio[r] = ret[1][r];
							}
						} else {
							e.residence = null;
							e.residenceRatio = null;
						}
						if (e.residence == null) // site == -1
							continue;
						int moveflag=0;

						for (int t = 0; t < streamNum; t++) {
							//System.out.println("============"+i+"\t"+t+"================");
							Meta[] pt = prune2(p, t);
							int flagfeature = 1;
							e.time[t][0]=pt[0].time;
							e.time[t][1]=pt[pt.length-1].time;
							if(pt[pt.length-1].state==0){
								e.moveornot[t]=false;
								moveflag=0;
							}
							else if(pt[pt.length-1].state==1){
								e.moveornot[t]=true;
								moveflag=1;
							}
							System.out.println(moveflag);
							for (int lu = 0; lu < pt.length; lu++) {
								if (sp.site.get(pt[lu].site).longitude < 120.53695
										|| sp.site.get(pt[lu].site).longitude > 120.900696)
									flagfeature = 0;
								if (sp.site.get(pt[lu].site).latitude < 27.846675
										|| sp.site.get(pt[lu].site).latitude > 28.162664)
									flagfeature = 0;
							}
							if (flagfeature == 0) {
								e.featureVector[t] = null;
								e.location[t] = null;
								continue;
							}
							
							if (pt.length > 0) {
								for (int f = 0; f < e.featureVector[t].length; f++) {
									e.featureVector[t][f] = 0.0f;
								}
								/////////////
								e.featureVector[t][fType.HomeLocX.ordinal()] = Residence_longitude;
								e.featureVector[t][fType.HomeLocY.ordinal()] = Residence_latitude;

								MobilityEntropy.entropy(pt, e.featureVector[t]);
								gi.getGeoInfo(pt, e.featureVector[t]);
								BasicDBObject doc=new BasicDBObject();
								ArrayList reocords=new ArrayList<>();
								for(int k=0;k<pt.length;k++){
									ArrayList record=new ArrayList<>();
									record.add(pt[k].longitude);
									record.add(pt[k].latitude);
									record.add(pt[k].time);
									reocords.add(record);
									
								}
								
								doc.append("userid", pt[0].userid).
									append("validFrameCount", validFrameCount).
									append("starttime", pt[0].time).
									append("endtime", pt[pt.length-1].time).
									append("HomeLocX",Residence_longitude).
									append("HomeLocY",Residence_latitude).
									append("records", reocords).
									append("movestate", moveflag);
								module.coll2.insert(doc);
								
								/////////////
								validFrameCount++;
							} else {
								System.out.println("hehedahehedaheheda");
								e.featureVector[t] = null;
							}

						}

						int flag = 0;
						int mflag=0;
						int sflag=0;
						for (int ii = 0; ii < e.featureVector.length; ii++) {
							if (e.featureVector[ii] != null)
								flag++;
						}
						
						
						if (flag > 0) {
							datawriter.writeLong(userid);
							datawriter.writeInt(rec_size);
							for (int ii = 0; ii < rec_size; ii++) {
								datawriter.writeLong(p[ii].time);
								datawriter.writeInt(p[ii].site);
							}
							
								
							validTrajCount++;
							element.add(e);

						}

					}
				}

				// System.out.println("************");
				if (i % 10 == 0 && i != 0) {
					System.out.println("**** " + i);
				}
			}

			System.out.print("\n");

			//in.close();
			datawriter.close();

			codewords = element.toArray(new Element[0]);
			System.out.println(validTrajCount + " ### " + validFrameCount + " ### ");
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

	}

	public Meta[] prune2(Meta[] p,int t){//提取出子记录序列
		ArrayList<Meta> tmp=new ArrayList<Meta>(p.length);
		int prestate=-1;
		int i=-1;//段数
		int count=0;
		while(i<t){//为了跳到那一段
			if(prestate<0){
				i++;
				count++;
				prestate=p[0].state;
			}
			else if(prestate==0){
				if(p[count].state==0){
					count++;
				}else if(p[count].state==1){
					i++;
					count++;
					prestate=1;
				}
			}else if(prestate==1){
				if(p[count].state==1){
					count++;
				}else if(p[count].state==0){
					i++;
					count++;
					prestate=0;
				}
			}
		}
		
		int start=0;
		start = count - 1;
		System.out.println(start);
		
		if(p[start].state==0){
			prestate=0;
			while(prestate==0&&start<p.length&&p[start].state==0){
				tmp.add(p[start]);
				prestate=p[start].state;
				start++;
			}
		}else if(p[start].state==1){
			prestate=1;
			while(prestate==1&&start<p.length&&p[start].state==0){
				tmp.add(p[start]);
				prestate=p[start].state;
				start++;
			}
		}
		return tmp.toArray(new Meta[0]);
	}

	public boolean isValid(Meta[] p) {
		// rule1: data should be valid more than 4 days
		// rule2: data should be more than 20 samples
		if (p.length > 0){
			if ((p[p.length - 1].time - p[0].time) > 4 * 86400000L && p.length > 20)
				return true;
			else
				return false;
		}else {
			return false;
		}
		
	}

	public void writeFile(String path) {

		int timeidcount=337;
		int[][] statetimecount=new int[timeidcount][2];
		try {
			String filename="Matlab";
			if(databasename=="mobiledata_test")
				filename="Matlab_test";
			DataOutputStream datawriter = new DataOutputStream(
						new BufferedOutputStream(new FileOutputStream("data/"+filename)));
			
			File file = new File("data/state.csv");
	        FileOutputStream out = new FileOutputStream(file);
	        OutputStreamWriter osw = new OutputStreamWriter(out, "UTF8");
	        BufferedWriter bw = new BufferedWriter(osw);
			datawriter.writeInt(codewords.length);
			datawriter.writeInt(cm.clusternum);
			int frameindex = 0;
			for (int i = 0; i < codewords.length; i++) {
				long userid=codewords[i].id;

				datawriter.writeInt(codewords[i].featureVector.length);
				for (int j = 0; j <  codewords[i].featureVector.length; j++) {// number															//														// frames
					if (codewords[i].featureVector[j] != null) {
						for (int k = 0; k < cm.clusternum; k++) {
							datawriter.writeFloat(codewords[i].value[j][k]);
						}
					} else {
						for (int k = 0; k < cm.clusternum; k++) {
							float f = 0;
							datawriter.writeFloat(f);
						}
					}
				}
				for (int j = 0; j < codewords[i].time.length; j++) {// number															//														// frames
					if (codewords[i].time[j] != null) {

						Long starttime=codewords[i].time[j][0];
						Long endtime=codewords[i].time[j][1];
						float starthour=(float) ((starttime-stime)/(1000*60*60*1.0));
						float endhour=(float) ((endtime-stime)/(1000*60*60*1.0));
						
						datawriter.writeFloat(starthour);
						datawriter.writeFloat(endhour);
						int timeid=(int) Math.floor((endtime-starttime)/(1000*60*30));
						if(timeid>=timeidcount-1){
							timeid=timeidcount-1;
							System.out.println("invalid: "+i+" , "+j);
						}
						if(codewords[i].moveornot[j])
							statetimecount[timeid][1]++;
						else
							statetimecount[timeid][0]++;
						frameindex++;
					} else {
						for (int k = 0; k < cm.clusternum; k++) {
							float f = 0;
							System.err.println("ERRRRRROR!");
							datawriter.writeFloat(f);
						}
					}

				}
			}
			datawriter.close();

			bw.write("timeduration"+","+"staycount"+","+"movecount"+"\r\n");
			for(int k=0;k<timeidcount;k++){

				bw.write(k+","+statetimecount[k][0]+","+statetimecount[k][1]+"\r\n");
			}
			bw.close();
			osw.close();
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Matlab写入结束");

	}

	public void writemysql() throws SQLException{
		
		int timepiececount=(int) ((etime-stime)/interval);

		int[][][] countwithouttime=new int[cm.clusternum][fType.values().length][catalogs];
		int[][][][] clusterstatistics=new int[timepiececount][cm.clusternum][fType.values().length][catalogs];
		int[][] clustertotal=new int[fType.values().length][catalogs];
		float [][] clusteravg=new float[cm.clusternum][fType.values().length];
		int [][] clustercount=new int[cm.clusternum][fType.values().length];
		int [][][] statecount=new int[timepiececount][cm.clusternum][2];

		int [] nodesize=new int[cm.clusternum];
		String [][] nodeuser=new String[timepiececount][cm.clusternum];
		String [] nodeuserwithouttime=new String[cm.clusternum];
		System.out.println("starting write mysql");
		System.out.println();
		System.out.println("starting write trajectory");
		SQLModule sqlModule=new SQLModule();
		sqlModule.connect();
		String tableName = databasename+ "." + "clustertrajectoryw";
		String truncateString="TRUNCATE TABLE ";
		
		Statement stmt = sqlModule.conn.createStatement();
		System.out.println("Clear Old Data ...");
		stmt.execute(truncateString+ databasename + ".clustertrajectory");
		stmt.execute(truncateString+ databasename + ".clusterstatistics");
		stmt.execute(truncateString+ databasename + ".clusterrange");
		stmt.execute(truncateString+ databasename + ".clustertrajwithouttime");
		stmt.execute(truncateString+ databasename + ".clusterstatisticswithoutt");
		stmt.execute(truncateString+ databasename + ".clustertotal");
		stmt.execute(truncateString+ databasename + ".clusteravg");
		stmt.execute(truncateString+ databasename + ".statecount");
		stmt.execute(truncateString+ databasename + ".usercountdata");
		stmt.execute(truncateString+ databasename + ".nodeusers");
		stmt.execute(truncateString+ databasename + ".nodeuserswithouttime");
		stmt.execute(truncateString+ databasename + ".nodesize");

		System.out.println();
		stmt.execute("DROP INDEX inddex on "+ databasename + ".clustertrajectory");
		stmt.execute("DROP INDEX inddex on "+ databasename + ".clusterstatistics");
		stmt.execute("DROP INDEX inddex on "+ databasename + ".clustertrajwithouttime");
		stmt.execute("DROP INDEX inddex on "+ databasename + ".clusterstatisticswithoutt");
		stmt.execute("DROP INDEX inddex on "+ databasename + ".clustertotal");
		stmt.execute("DROP INDEX inddex on "+ databasename + ".clusteravg");
		stmt.execute("DROP INDEX inddex on "+ databasename + ".statecount");
		stmt.execute("DROP INDEX inddex on "+ databasename + ".usercountdata");
		stmt.execute("DROP INDEX inddex on "+ databasename + ".nodeusers");
		stmt.execute("DROP INDEX inddex on "+ databasename + ".nodeuserswithouttime");
		stmt.execute("DROP INDEX inddex on "+ databasename + ".nodesize");

		System.out.println("Clear Old Data finish");
		System.out.println();
		String insert_sql1="INSERT INTO "+databasename+".clustertrajectory VALUES (?,?,?,?,?)";
		
	    PreparedStatement psts = (PreparedStatement) sqlModule.conn.prepareStatement(insert_sql1);

		int total=0;
		Long t =System.currentTimeMillis();
		for(long i=stime;i<etime;i=i+interval){
			long time=i;
			
				BasicDBObject queryObject = new BasicDBObject().append("starttime",  
		                new BasicDBObject().append(QueryOperators.LTE, i)).append("endtime", new BasicDBObject().append(QueryOperators.GTE, i));
				DBCursor cursor=module.coll2.find(queryObject);
				total+=cursor.count();
				ArrayList<ArrayList<SQLInsert>>result=new ArrayList<ArrayList<SQLInsert>>();
				if(cursor.count()==0)
					continue;
				while(cursor.hasNext()){
					
					DBObject dbj=cursor.next();
					SQLInsert insert=new SQLInsert();
					int validFrameCount=(Integer)dbj.get("validFrameCount");
					int clusterid=cm.data.labels[validFrameCount];
					int moveflag=(Integer)dbj.get("movestate");
					long userid=(long)dbj.get("userid");
					int timeindex=(int) ((time-stime)/interval);

					double[] codewords=new double[fType.values().length];
					ArrayList reocords=(ArrayList) dbj.get("records");
					String traj="";
					
					
					if(moveflag==1)
						statecount[timeindex][clusterid][1]++;
					else 
						statecount[timeindex][clusterid][0]++;
					for(int id=0;id<cm.data.data[validFrameCount].length;id++){
						int dim=(int) Math.floor(cm.data.data[validFrameCount][id]*(catalogs*1.0));
						if(dim==catalogs)
							dim=catalogs-1;
						clusterstatistics[timeindex][cm.data.labels[validFrameCount]][id][dim]++;
					}
					
					for(int w=0;w<cm.clusternum;w++){
						result.add(new ArrayList<SQLInsert>());
					}
					for(int s=0;s<reocords.size()-1;s++){
						ArrayList record=(ArrayList)reocords.get(s);
						traj=traj+"["+Double.valueOf((double)record.get(0))+","+Double.valueOf((double)record.get(1));
						traj=traj+"],";
					}
					ArrayList record=(ArrayList)reocords.get(reocords.size()-1);
					traj=traj+"["+Double.valueOf((double)record.get(0))+","+Double.valueOf((double)record.get(1));
					traj=traj+"]";
					for (int s = 0; s <fType.values().length ; s++) {
						codewords[s]=cm.data.data[validFrameCount][s];
					}
					
					double distance=Kmeans.dist(codewords,cm.data.centers[clusterid],fType.values().length);
					insert.clusterid=clusterid;
					insert.time=time;
					insert.traj=traj;
					insert.distance=distance;
					insert.state=moveflag;
					result.get(clusterid).add(insert);
				}
				if(result==null)
					continue;
				for(int s=0;s<cm.clusternum;s++){
					if(result.get(s).size()!=0){
						String sql= "insert into "+tableName+" values";
							Collections.sort(result.get(s), new SortByDis());
							for(int ss=0;ss<result.get(s).size();ss++){
								int timeindex=(int) ((result.get(s).get(ss).time-stime)/interval);
								psts.setInt(1, result.get(s).get(ss).clusterid);
								psts.setInt(2, timeindex);
								psts.setInt(3, ss);
								psts.setString(4, "\"["+result.get(s).get(ss).traj+"]\"");
								psts.setInt(5, result.get(s).get(ss).state);
								psts.addBatch();
							}
					}
				}
		}
		psts.executeBatch();
		System.out.println("write trajectory end\ncost: "+(System.currentTimeMillis()-t)/1000.0);
		System.out.println();
		
		String insert_sql2="INSERT INTO "+databasename+".clusterstatistics VALUES (?,?,?,?,?)";
		String insert_sql3="INSERT INTO "+databasename+".clustertrajwithouttime VALUES (?,?,?,?)";
		String insert_sql4="INSERT INTO "+databasename+".clusterstatisticswithoutt VALUES (?,?,?,?)";
		String insert_sql5="INSERT INTO "+databasename+".usercountdata VALUES (? ,? ,? ,? ,? , ? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? "
																			+ ",? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? "
																				+ ",? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,?)";

	    PreparedStatement psts2 = (PreparedStatement) sqlModule.conn.prepareStatement(insert_sql5);
	    psts=(PreparedStatement) sqlModule.conn.prepareStatement(insert_sql3);
		System.out.println("starting count clusters without time");
		t=System.currentTimeMillis();
		ArrayList<ArrayList<SQLInsert>>resultArrayList=new ArrayList<ArrayList<SQLInsert>>();
		DBCursor dbCursor=module.coll2.find();
		for(int w=0;w<cm.clusternum;w++){
			resultArrayList.add(new ArrayList<SQLInsert>());
			nodeuserwithouttime[w]="";
			for(int ww=0;ww<timepiececount;ww++){
				nodeuser[ww][w]="";
			}
		}

		long preuserid=-1;
		String clusterreocrd="";
		int datacount=0;
		int timecount[]=new int[timepiececount];
		for(int i=0;i<84;i++){
			timecount[i]=-1;
		}
		double[] dataavg=new double [fType.values().length];
		Double prehomelocx = 0.0;
		Double prehomelocy= 0.0;
		while(dbCursor.hasNext()){
			DBObject dbj=dbCursor.next();
			int validFrameCount=(Integer)dbj.get("validFrameCount");
			long userid=(long)dbj.get("userid");
			int clusterid=cm.data.labels[validFrameCount];
			int moveflag=(Integer)dbj.get("movestate");
			long starttime=(long)dbj.get("starttime");
			long endtime=(long)dbj.get("endtime");

			int startid=(int)Math.ceil((starttime-stime)/interval);
			int endid=(int)Math.floor((endtime-stime)/interval);
			Double homelocx=(Double)dbj.get("HomeLocX");
			Double homelocy=(Double)dbj.get("HomeLocY");	
			double[] codewords=new double[fType.values().length];
			ArrayList reocords=(ArrayList) dbj.get("records");
			nodesize[clusterid]++;
			if(nodeuserwithouttime[clusterid]==""){
				nodeuserwithouttime[clusterid]+=userid;
			}else{
				nodeuserwithouttime[clusterid]+=","+userid;
			}
			String traj="";
			String tmp="";
			if (startid<0)
				startid=0;
			if(endid>83)
				endid=83;

			if(startid<=endid){
				for(int s=startid;s<endid;s++){
					if(nodeuser[s][clusterid].length()==0){
						nodeuser[s][clusterid]+=userid;
					}
					else{
						nodeuser[s][clusterid]+=","+userid;
					}
					tmp+="["+s+","+String.valueOf(clusterid)+"],";
					if(preuserid==userid||preuserid==-1)
						timecount[s]=clusterid;
				}
			}
			if(preuserid==-1){
				clusterreocrd+=tmp;
				preuserid=userid;
				prehomelocy=homelocy;
				prehomelocx=homelocx;
				for(int k=0;k<fType.values().length;k++){
					dataavg[k]+=cm.data.data[validFrameCount][k];
				}
				datacount++;
				
			}
			else if(preuserid==userid){
				prehomelocy=homelocy;
				prehomelocx=homelocx;
				for(int k=0;k<fType.values().length;k++){
					dataavg[k]+=cm.data.data[validFrameCount][k];
				}
				datacount++;
				clusterreocrd+=tmp;
			}else{
				clusterreocrd="\"["+clusterreocrd.substring(0,clusterreocrd.length()-1)+"]\"";
				long tmpp=preuserid;
				String featurevectoravg=String.valueOf((1.0*dataavg[0]/datacount));
				dataavg[0]=0;
				for(int k=1;k<fType.values().length;k++){
					featurevectoravg+=","+(1.0*dataavg[k]/datacount);
					dataavg[k]=0;
				}
				psts2.setLong(1, tmpp);
				psts2.setString(2, clusterreocrd);
				psts2.setString(3, featurevectoravg);
				psts2.setDouble(4, prehomelocx);
				psts2.setDouble(5, prehomelocy);
				for(int i=0;i<84;i++){
					psts2.setInt(6+i, timecount[i]);
				}
				psts2.addBatch();

				for(int i=0;i<84;i++){
					timecount[i]=-1;
				}
				if(startid<=endid){
					for(int i=startid;i<endid;i++){
						timecount[i]=clusterid;
					}
				}
				prehomelocy=homelocy;
				prehomelocx=homelocx;
				preuserid=userid;
				datacount=1;
				for(int k=0;k<fType.values().length;k++){
					dataavg[k]+=cm.data.data[validFrameCount][k];
				}
				clusterreocrd=tmp;
			}

			for(int id=0;id<cm.data.data[validFrameCount].length;id++){
				int dim=(int) Math.floor(cm.data.data[validFrameCount][id]*(catalogs*1.0));
				if(dim==catalogs)
					dim=catalogs-1;
				countwithouttime[cm.data.labels[validFrameCount]][id][dim]++;
				clustertotal[id][dim]++;
				clusteravg[cm.data.labels[validFrameCount]][id]+=cm.data.data[validFrameCount][id];
				clustercount[cm.data.labels[validFrameCount]][id]++;
			}
			for(int s=0;s<reocords.size()-1;s++){
				ArrayList record=(ArrayList)reocords.get(s);
				traj=traj+"["+Double.valueOf((double)record.get(0))+","+Double.valueOf((double)record.get(1));
				traj=traj+"],";
			}
			ArrayList record=(ArrayList)reocords.get(reocords.size()-1);
			traj=traj+"["+Double.valueOf((double)record.get(0))+","+Double.valueOf((double)record.get(1));
			traj=traj+"]";
			for (int s = 0; s <fType.values().length ; s++) {
				codewords[s]=cm.data.data[validFrameCount][s];
			}
			double distance=Kmeans.dist(codewords,cm.data.centers[clusterid],fType.values().length);
			SQLInsert insert=new SQLInsert();
			insert.clusterid=clusterid;
			insert.time=(long) -1;
			insert.traj=traj;
			insert.distance=distance;
			insert.state=moveflag;
			resultArrayList.get(clusterid).add(insert);
		}
		for(int i=0;i<cm.clusternum;i++){
			Collections.sort(resultArrayList.get(i),new SortByDis());
			int count=resultArrayList.get(i).size()<1000?resultArrayList.get(i).size():1000;
			for(int j=0;j<count;j++){
				psts.setInt(1, resultArrayList.get(i).get(j).clusterid);
				psts.setInt(2, j);
				psts.setString(3, "\"["+resultArrayList.get(i).get(j).traj+"]\"");
				psts.setInt(4, resultArrayList.get(i).get(j).state);
				psts.addBatch();
			}
		}
		psts.executeBatch();
		psts2.executeBatch();

		System.out.println("count clusters without time end\ncost: "+(System.currentTimeMillis()-t)/1000.0);
		System.out.println();
		
		psts = (PreparedStatement) sqlModule.conn.prepareStatement(insert_sql2);
		System.out.println("start writing clusterstatistics");
		t=System.currentTimeMillis();
		for (int i=0;i<clusterstatistics.length;i++)
			for(int j=0;j<clusterstatistics[i].length;j++)
				for(int k=0;k<clusterstatistics[i][j].length;k++)
					for(int m=0;m<clusterstatistics[i][j][k].length;m++){	
						psts.setInt(1, i);
						psts.setInt(2, j);
						psts.setInt(3, k);
						psts.setInt(4, m);
						psts.setInt(5, clusterstatistics[i][j][k][m]);
						psts.addBatch();
					}
		psts.executeBatch();
		System.out.println("write clusterstatistics end\ncost: "+(System.currentTimeMillis()-t)/1000.0);
		System.out.println();
		
		
		System.out.println("start writing clusterstatistics without time");
		psts = (PreparedStatement) sqlModule.conn.prepareStatement(insert_sql4);
		t=System.currentTimeMillis();
		for(int i=0;i<cm.clusternum;i++)
			for(int j=0;j<countwithouttime[i].length;j++)
				for(int k=0;k<countwithouttime[i][j].length;k++){	
					psts.setInt(1, i);
					psts.setInt(2, j);
					psts.setInt(3, k);
					psts.setInt(4, countwithouttime[i][j][k]);
					psts.addBatch();
				}
		psts.executeBatch();
		System.out.println("write clusterstatistics without time end\ncost: "+(System.currentTimeMillis()-t)/1000.0);
		System.out.println();
		
		String sql="INSERT INTO "+databasename+".clustertotal VALUES (?,?,?)";
		psts = (PreparedStatement) sqlModule.conn.prepareStatement(sql);
		System.out.println("start writing clusterstatistics total");
		t=System.currentTimeMillis();
		for(int i=0;i<fType.values().length;i++)
			for(int j=0;j<clustertotal[i].length;j++){
				psts.setInt(1, i);
				psts.setInt(2, j);
				psts.setInt(3, clustertotal[i][j]);
				psts.addBatch();
			}
		psts.executeBatch();
		System.out.println("write clusterstatistics total end\ncost: "+(System.currentTimeMillis()-t)/1000.0);
		System.out.println();
		

		System.out.println("start writing node size count");
		sql="INSERT INTO "+databasename+".nodesize VALUES (?,?)";
		psts = (PreparedStatement) sqlModule.conn.prepareStatement(sql);
		t=System.currentTimeMillis();
		for(int i=0;i<cm.clusternum;i++){
			psts.setInt(1, i);
			psts.setInt(2, nodesize[i]);
			psts.addBatch();
		}
		psts.executeBatch();
		System.out.println("write node size count end\ncost: "+(System.currentTimeMillis()-t)/1000.0);
		System.out.println();

		
		System.out.println("start writing clusterstatistics average");
		sql="INSERT INTO "+databasename+".clusteravg VALUES (?,?,?)";
		psts = (PreparedStatement) sqlModule.conn.prepareStatement(sql);
		t=System.currentTimeMillis();
		for(int i=0;i<cm.clusternum;i++)
			for(int j=0;j<clusteravg[i].length;j++){
				float tmp=(float) (clusteravg[i][j]/clustercount[i][j]);
				psts.setInt(1, i);
				psts.setInt(2, j);
				psts.setFloat(3, tmp);
				psts.addBatch();			
			}
		psts.executeBatch();
		System.out.println("write clusterstatistics average end\ncost: "+(System.currentTimeMillis()-t)/1000.0);
		System.out.println();
		
		System.out.println("start writing state count");
		sql="INSERT INTO "+databasename+".statecount VALUES (?,?,?,?)";
		psts = (PreparedStatement) sqlModule.conn.prepareStatement(sql);
		t=System.currentTimeMillis();
		for(int i=0;i<timepiececount;i++){
			for(int j=0;j<statecount[i].length;j++){
				float tmp;
				if(statecount[i][j][0]+statecount[i][j][1]==0)
					tmp=0;
				else
					tmp=(float) (statecount[i][j][1])/(statecount[i][j][0]+statecount[i][j][1]);
				int count=statecount[i][j][0]+statecount[i][j][1];
				psts.setInt(1, i);
				psts.setInt(2, j);
				psts.setFloat(3, tmp);
				psts.setInt(4, count);
				psts.addBatch();	
			}
		}
		psts.executeBatch();
		System.out.println("writing state count end\ncost: "+(System.currentTimeMillis()-t)/1000.0);
		System.out.println();
		
		System.out.println("start writing node users");
		sql="INSERT INTO "+databasename+".nodeusers VALUES (?,?,?)";
		psts = (PreparedStatement) sqlModule.conn.prepareStatement(sql);
		t=System.currentTimeMillis();
		for(int i=0;i<timepiececount;i++)
			for(int j=0;j<cm.clusternum;j++){
				psts.setInt(1, i);
				psts.setInt(2, j);
				psts.setString(3, nodeuser[i][j]);
				psts.addBatch();
			}
		psts.executeBatch();
		System.out.println("write node users end\ncost: "+(System.currentTimeMillis()-t)/1000.0);
		System.out.println();
		
		System.out.println("start writing node users without time ");
		sql="INSERT INTO "+databasename+".nodeuserswithouttime VALUES (?,?)";
		psts = (PreparedStatement) sqlModule.conn.prepareStatement(sql);
		t=System.currentTimeMillis();
		for(int i=0;i<cm.clusternum;i++){
			psts.setInt(1, i);
			psts.setString(2, nodeuserwithouttime[i]);
			psts.addBatch();			
		}
		psts.executeBatch();
		System.out.println("write node users without time end\ncost: "+(System.currentTimeMillis()-t)/1000.0);
		System.out.println();
		
		
		System.out.println("start writing clusterrange");
		t=System.currentTimeMillis();
		sql="insert into "+databasename+".clusterrange values( ";
		for(int i=0;i<9;i++){
			String tmp="\"["+String.valueOf(cm.range[i][0])+","+String.valueOf(cm.range[i][1])+"]\",";
			sql=sql+tmp;
		}
		String tmp="\"["+String.valueOf(cm.range[9][0])+","+String.valueOf(cm.range[9][1])+"]\");";
		sql=sql+tmp;
		stmt.execute(sql);	
		System.out.println("write clusterrange end\ncost: "+(System.currentTimeMillis()-t)/1000.0);
		System.out.println();

		System.out.println("creating indexes");
		t=System.currentTimeMillis();

		stmt.execute("CREATE INDEX inddex ON "+databasename+"."+"clustertrajectory (clusterid,timeid)");
		stmt.execute("CREATE INDEX inddex ON "+databasename+"."+"clusterstatistics (timeindex,clusterindex)");
		stmt.execute("CREATE INDEX inddex ON "+databasename+"."+"clustertrajwithouttime (clusterid)");
		stmt.execute("CREATE INDEX inddex ON "+databasename+"."+"clusterstatisticswithoutt (clusterid)");
		stmt.execute("CREATE INDEX inddex ON "+databasename+"."+"clustertotal (dimindex)");
		stmt.execute("CREATE INDEX inddex ON "+databasename+"."+"clusteravg (clusterid)");
		stmt.execute("CREATE INDEX inddex ON "+databasename+"."+"statecount (timeindex,clusterid)");
		stmt.execute("CREATE INDEX inddex ON "+databasename+"."+"usercountdata (userid)");
		stmt.execute("CREATE INDEX inddex ON "+databasename+"."+"nodeusers (clusterid, timeid)");
		stmt.execute("CREATE INDEX inddex ON "+databasename+"."+"nodeuserswithouttime (clusterid)");
		stmt.execute("CREATE INDEX inddex ON "+databasename+"."+"nodesize (clusterid)");
		
		
		//System.out.println(sql);
		System.out.println("creating indexes end\ncost: "+(System.currentTimeMillis()-t)/1000.0);
		System.out.println();
		
		//module.coll2.remove(new BasicDBObject());
		
		sqlModule.conn.commit();
		sqlModule.disconnect();

	}

	public static void main(String[] in){
		CodeWordsGenerator cg = new CodeWordsGenerator();
		cg.writeFile("data/trajfile.txt");
		try {
			cg.writemysql();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			System.out.println("程序运行完成");
			System.out.println();
		}
}
