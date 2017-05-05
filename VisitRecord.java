package visitRecord;

/*
用于统计data文件夹下各个监测点当天的到访记录
思路：每次统计到访记录之前，先删除当天的到访记录，因为文件的实时性增长和数据的记忆�?�?
循环扫描各个监测点下的数据文件，统计当天的到访记录；
每统计完�?��监测点的当天数据，将统计结果导入数据表visitrecord�?
*/
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.regex.Pattern;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class VisitRecord{
	private static Logger logger = LogManager.getLogger(VisitRecord.class.getName());
	
	public static final String DBDRIVER   ="com.mysql.jdbc.Driver";		
	public static final String DBURL      ="jdbc:mysql://localhost:3306/wibupt";
	private static Connection conn        = null;
	
	public  Connection connDatabase(String user,String passwd) throws SQLException, ClassNotFoundException{
		try {
			Class.forName(DBDRIVER);
		} catch (ClassNotFoundException e) {
			throw e;
		}							
		String DBUSER     =user;						
	    String DBPASSEORD =passwd;						
		conn = DriverManager.getConnection(DBURL,DBUSER,DBPASSEORD);																	
		return conn;											
	}
	
	public File datafolder; 	//存储原始文件的�?目录
	public File[] monfolders; 	//原始文件总目录下的子目录，即各个监测点的数据文件�?
	
	class VisitRecordData {
		 public String mac;
		 public int intime;
		 public int offtime;
	}
	
	//循环扫描各个监测点下的数据文件，统计当天的到访记录；每统计完�?��监测点的当天数据，将统计结果导入数据表visitrecord
	public void torun(String datafilepath, int twiceinterval, String day) {
		datafolder = new File(datafilepath);
		if (datafolder == null || !(datafolder.exists())){  //判断存储原始数据的目录是否存�?
			logger.error("Folder: "+datafilepath+"  not found~~ To WAIT the next cycle!");
			return;
		}

		monfolders = datafolder.listFiles(); // 列出data总目录下的所有监测点的data目录
		if(monfolders == null || monfolders.length == 0) {
			logger.error("No folder found in " + datafolder.getName() + "~~ To QUIT!");
			return;
		}

		if(day.equals("today")) day = getTodayStr(); // 如果�?today"，需要转换成�?20140721 这样的字符串
			
		for (int i = 0 ; i< monfolders.length;i++) {
    		double[] rss;
			try {
				rss = getrss(monfolders[i].getName());
			} catch (SQLException e) {
				logger.error("SQLException when retrieve RSS of:" + monfolders[i].getName() +"~~ To IGNORE it's data!");
				continue;
			}
			if(rss==null) {
				logger.error("Null returned when retrieve RSS of:" + monfolders[i].getName() +"~~ To IGNORE it's data!");
				continue;
			}
			double rss_in = rss[0];  // 该monid设定的室内信号强度的小�?，大于rss_in的信号都认为是室内手机发出的信号
			double rss_out = rss[1]; // 周边信号强度的小值，大于rss_out的信号都认为是室内手机发出的信号
			
			logger.info("Begin to analysis: " + monfolders[i].getName() + " with rss_in:" 
									+ rss_in + "dBm and rss_out:" + rss_out + "dBm");

			//打开监测点目录下某天文件~~只处理当天的数据
			File dayfile = new File(monfolders[i].getPath()+ System.getProperty("file.separator") + day);
			if ( dayfile==null || !(dayfile.exists()) ){	//判断文件是否存在
				logger.info("The data file " + dayfile.getName() + " not found!~~ To IGNORE it's data!");
				continue;    //若没有当天数据，接着处理下一个监测点的数�?
			}
				
			// 逐行处理
			FileInputStream fis = null;
			Scanner monscan = null;
			try {
				fis = new FileInputStream(dayfile);
			} catch (FileNotFoundException e) {
				logger.error("FileNotFoundException!! " + dayfile.getName() + " ~~ To IGNORE it's data!");
				continue;    //若没有当天数据，接着处理下一个监测点的数�?
			}
			monscan = new Scanner(fis);
				
			Set<VisitRecordData> in_visitrecordset = new HashSet<VisitRecordData>();
			Map<String, VisitRecordData> in_intime_mac_map = new HashMap<String, VisitRecordData>();
			
			// 1. get the latest intime through query the last 30 records.
			int latestInTime = 0;
			ArrayList<Integer> inTimeList = null;
			try {
				String queryLastRecords = "SELECT inTime FROM (SELECT inTime, monid FROM visitrecord ORDER BY ID DESC LIMIT 200000) AS subvisitrecord WHERE monid='" + monfolders[i].getName() + "'";
				PreparedStatement prepStmt = conn.prepareStatement(queryLastRecords);
				ResultSet queryResultSet = prepStmt.executeQuery();
				inTimeList = new ArrayList<Integer>();
				while (queryResultSet.next()) {
					inTimeList.add(queryResultSet.getInt(1));
				}
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			
			for (Integer inTime: inTimeList) {
				if (inTime > latestInTime) {
					latestInTime = inTime;
				}
			}
			
			// 2. 先定位到文件上次处理完的那行
			if (latestInTime != 0) {
				// 3. 判断是昨天还是今天
				long lastestInTime = latestInTime * 1000L;
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
				String lastTimeDay = sdf.format(new Date(lastestInTime));
				if (lastTimeDay.equals(day)) {
					while (monscan.hasNextLine()) {
						String line = monscan.nextLine();
						Pattern subpat=Pattern.compile("[|]") ;	
						String mac_rss_time[]=subpat.split(line);	
						int linetime = Integer.parseInt(mac_rss_time[2]);
						if (linetime == latestInTime) {
							break;
						}
					}
				}
			}
			
//			System.out.println("定位读取位置花费:" + (System.currentTimeMillis() - start));
			
//			start = System.currentTimeMillis();
				
			while (monscan.hasNextLine()){
				String line = monscan.nextLine();
				Pattern subpat=Pattern.compile("[|]") ;			//创建Pattern实例
				String mac_rss_time[]=subpat.split(line);	
				int linetime   =Integer.parseInt(mac_rss_time[2]); // time
				int linerss    =Integer.parseInt(mac_rss_time[1]); // rss
				String linemac = mac_rss_time[0];
				
				// 室内visitrecord处理
				if( linerss > rss_in) {
					if( !in_intime_mac_map.containsKey(linemac) ) { // 如果没有这个mac的记录，说明是一个新的visitrecord
						VisitRecordData vrd = new VisitRecordData();
						vrd.mac = linemac;
						vrd.intime = linetime;  // 设定intime
						vrd.offtime = linetime + 1;
						in_intime_mac_map.put(linemac, vrd);   // 记录该MAC的intime
						// 没有的直接加入set中
						in_visitrecordset.add(vrd);
					}
					else {
						VisitRecordData tempvrd = ((VisitRecordData)in_intime_mac_map.get(linemac));
						if( linetime <  tempvrd.offtime + twiceinterval ) {
							if(linetime > tempvrd.offtime) { // 新的时间必须比之前的数据时间大，以排除重复数据对offtime的影�?
								tempvrd.offtime = linetime; // 更新offtimce
							}
						}
						else {
							in_visitrecordset.add(tempvrd); // 将该MAC的访问记录插入set，用于Add到数据库
							
							in_intime_mac_map.remove(linemac);
							
							VisitRecordData vrd = new VisitRecordData(); 
							vrd.mac = linemac;
							vrd.intime = linetime;  // 设定intime
							vrd.offtime = linetime + 1;
							in_intime_mac_map.put(linemac, vrd);   // 记录该MAC的新的intime
						}
					}
				}

	/*			if( linerss > rss_out) {
					// 周边visitrecord处理
					if( !all_intime_mac_map.containsKey(linemac) ) { // 如果没有这个mac的记录，说明是一个新的visitrecord
						VisitRecordData vrd = new VisitRecordData();
						vrd.mac = linemac;
						vrd.intime = linetime;  // 设定intime
						vrd.offtime = linetime + 1;
						all_intime_mac_map.put(linemac, vrd);   // 记录该MAC的intime
					}
					else {
						VisitRecordData tempvrd = ((VisitRecordData)all_intime_mac_map.get(linemac));
						if( linetime <  tempvrd.offtime + twiceinterval ) {
							if(linetime > tempvrd.offtime) { // 新的时间必须比之前的数据时间大，以排除重复数据对offtime的影�?
								tempvrd.offtime = linetime; // 更新offtimce
							}
						}
						else {
							all_visitrecordset.add(tempvrd); // 将该MAC的访问记录插入set，用于Add到数据库
							
							all_intime_mac_map.remove(linemac);
							
							VisitRecordData vrd = new VisitRecordData();
							vrd.mac = linemac;
							vrd.intime = linetime;  // 设定intime
							vrd.offtime = linetime + 1;
							all_intime_mac_map.put(linemac, vrd);   // 记录该MAC的新的intime
	//							System.out.println("insert a new intime visit record");
						}
					}
				}*/
			} // end while
			monscan.close();
			try {
				if(fis!=null) fis.close();
			} catch (IOException e) {
				logger.error(e.getMessage());
				continue;
			}
			
//			long endprocessData = System.currentTimeMillis();
			
//			System.out.println("处理数据时间" + (endprocessData - start));
			
//			start = System.currentTimeMillis();

			// 批量插入数据�?
//			clearDB(day, monfolders[i].getName()); // 先清空当前的到访记录
			
//			endprocessData = System.currentTimeMillis();
			
//			System.out.println("删除数据库时间" + (endprocessData - start));
			
//			start = System.currentTimeMillis();

			try {
				conn.setAutoCommit(false);
//				java.sql.Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, 
//				                                    ResultSet.CONCUR_READ_ONLY);  
				Iterator<VisitRecordData> in_it = in_visitrecordset.iterator(); 
//				System.out.println("数据量多少：" + in_visitrecordset.size());
				String sql = "INSERT INTO visitrecord(mac, inTime, offTime, dwellTime, monid) VALUES(?, ?, ?, ?, ?)";
				PreparedStatement ps = conn.prepareStatement(sql);
				while (in_it.hasNext()) {
					VisitRecordData vrd4db = in_it.next(); 
					int tempdwelltime = vrd4db.offtime - vrd4db.intime;
					ps.setString(1, vrd4db.mac);
					ps.setInt(2, vrd4db.intime);
					ps.setInt(3, vrd4db.offtime);
					ps.setInt(4, tempdwelltime);
					ps.setString(5, monfolders[i].getName());
					ps.addBatch();
//				    stmt.execute("INSERT INTO visitrecord(mac,inTime,offTime,dwellTime,monid) "
//				    		 + "VALUES('" + vrd4db.mac + "'," + vrd4db.intime + "," + vrd4db.offtime + ","
//				    		 + tempdwelltime +  ",'"+ monfolders[i].getName() + "')");
	//				    System.out.println("INSERT INTO visitrecord(mac,inTime,offTime,dwellTime,monid) "
	//				    		 + "VALUES('" + vrd4db.mac + "'," + vrd4db.intime + "," + vrd4db.offtime + ","
	//				    		 + tempdwelltime +  ",'"+ monfolders[i].getName() + "')");
				}
				logger.info("To insert " + in_visitrecordset.size() + " records into visitrecord!");
				ps.executeBatch();
				conn.commit();
				
				/*Iterator<VisitRecordData> all_it = all_visitrecordset.iterator(); 
				while (all_it.hasNext()) {
					VisitRecordData vrd4db = all_it.next(); 
					int tempdwelltime = vrd4db.offtime - vrd4db.intime;
				    stmt.execute("INSERT INTO visitrecord_all(mac,inTime,offTime,dwellTime,monid) "
				    		 + "VALUES('" + vrd4db.mac + "'," + vrd4db.intime + "," + vrd4db.offtime + ","
				    		 + tempdwelltime +  ",'"+ monfolders[i].getName() + "')");
	//				    System.out.println("INSERT INTO visitrecord_all(mac,inTime,offTime,dwellTime,monid) "
	//				    		 + "VALUES('" + vrd4db.mac + "'," + vrd4db.intime + "," + vrd4db.offtime + ","
	//				    		 + tempdwelltime +  ",'"+ monfolders[i].getName() + "')");
				} 
				logger.info("To insert " + all_visitrecordset.size() + " records into visitrecord_all!");
				conn.commit();*/
				logger.info("insert commit completed");
			}catch (SQLException e) {
				logger.error(e.getMessage());
				break; // 处理下一个监测点的数�?
			}
			
//			endprocessData = System.currentTimeMillis();
			
//			System.out.println("批量插入数据时间" + (endprocessData - start));
		}
	}
	
	public String getTodayStr() {
		long time=System.currentTimeMillis();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String today = sdf.format(new Date(time)); 
		return today; // 20140718
	}
	
	//删除当天到访记录
	public void clearDB(String _day, String monid) {
		long daytime = getgelin(_day);
//		long daytime = (System.currentTimeMillis()/1000);
//		daytime = daytime/(3600*24); // 86400 = 3600*24，即�?��的秒�?
//		daytime = daytime*24*3600; // 至此，得到当天零时零刻对应的格林威治时间的秒�?
		PreparedStatement perstat =null;
		long daytimeafter24hours = daytime + 24*3600;
		String in_sql = "delete from visitrecord where inTime > '"+daytime+"' and inTime <'" + daytimeafter24hours +"' and monid='" + monid + "';";
//		System.out.println("clearDB in: " + in_sql);
//		String all_sql ="delete from visitrecord_all where inTime > '"+daytime+"' and inTime <'" + daytimeafter24hours +"' and monid='" + monid + "';";
//		System.out.println("clearDB all: " + in_sql);
		try{
			conn.setAutoCommit(false);
			perstat = conn.prepareStatement(in_sql);						
			perstat.executeUpdate() ;
			//perstat = conn.prepareStatement(all_sql);						
		//	perstat.executeUpdate() ;
			conn.commit();
			//logger.info("Firstly, to clear " + _day + " data in visitrecord and visitrecord_all of " + monid);
		}catch (SQLException e) {
			logger.error(e.getMessage());
		}
	}
	
	// 将字符串转为时间�?
	// 20140712 --> 类似1405814899的数�?
	public long getgelin(String user_time) { 
		long re_time = 0; 
		String re_time1 = null; 
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd"); 
		Date d; 
		try { 
			d = sdf.parse(user_time); 
			long l = d.getTime(); 
			String str = String.valueOf(l); 
			re_time1 = str.substring(0, 10); 
			re_time =Long.parseLong(re_time1); 
		} catch (ParseException e) { 
			logger.error(e.getMessage());
		} 
		return  re_time;
	} 
	
	public double[] getrss(String monid) throws SQLException {
		double[] rss= new double[2];
		PreparedStatement perstat =null;								//声明PreparedStatement对象
		ResultSet res=null;
		String sql="select rssin,rssout FROM monindex where monid='"+monid+"';";
		try{
			perstat = conn.prepareStatement(sql);						//实例化Statement对象
			res = perstat.executeQuery() ;
			while (res.next()){	
				for (int i=0;i<2;i++)
				{
					rss[i]=res.getInt(i+1);
				}
			}
		}catch (SQLException e) {
			logger.error(e.getMessage());
			return null;
		}
		return rss;
	}
	
	public static void main(String[] args) throws SQLException {
		logger.info("visitrecord begin...");
		SignalHandler handler = new SignalHandler() {
			public void handle(Signal signal) {
				logger.error("capture close signal - " + signal.getName());
				if(conn!=null)
					try {
						conn.close();
					} catch (SQLException e) {
						logger.error(e.getMessage());
						System.exit(0);
					}
				System.exit(0);
			}
		};
		Signal.handle(new Signal("TERM"), handler);// 相当于kill -15
	    Signal.handle(new Signal("INT"), handler);// 相当于Ctrl+C
	    
		String user=args[0];//username,eg."root"
		String passwd=args[1];//password,eg."admin"
		String dataPath=args[2];//dataPath,eg."D:\\data"
//		int scaninterval=Integer.parseInt(args[3]);//interval(min),300
		int threhold=Integer.parseInt(args[3]);//threhold,eg.900, 同一MAC出现的间隔时间大于该门限，则认为是第2次到�?
		String datetoanalysis = args[4];//day,"20140525" or "today"
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");	
		try{
			VisitRecord visrec =new VisitRecord();
			conn =visrec.connDatabase(user,passwd);
			if(conn == null) {
				logger.error("Failed to get connection from mysql");
				System.exit(0);
			}
			logger.info("Database connection successfully!!  " + sdf.format(new Date()));
//			long start = System.currentTimeMillis();
			visrec.torun(dataPath, threhold, datetoanalysis);
//			System.out.println("花费时间为" + (System.currentTimeMillis() - start));
		}catch (SQLException e){
			logger.error(e.getMessage());
		} catch (ClassNotFoundException e) {
			logger.error(e.getMessage());
		}finally{
			logger.info("********FINALLY{} handling********");
			if(conn!=null)
				try {
					conn.close();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					logger.error(e1.getMessage());
				}
		}
	}
}


