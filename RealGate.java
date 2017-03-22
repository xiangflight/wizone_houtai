package com.wibupt.goandcome;
/*程序功能：统计当天校园活跃度；结果插入到DB中表activity中
 *       统计当天东、中、西、北四个门每天的进，出人次；结果插入到DB中表goandcome中
 *程序运行频率：每天一次
 * */
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RealGate {
	private static Logger logger = LogManager.getLogger(RealGate.class.getName());

	public static final String DBDRIVER = "com.mysql.jdbc.Driver"; // 定义数据库驱动程序
	public static final String DBURL = "jdbc:mysql://localhost:3306/wibupt"; // 定义数据库连接地址
	private static Connection conn = null; // 声明静态的Connection对象

	public class GateCount {
		String inside; // 此门校内的路由器的id
		String outside; // 此门校外的路由器的id
		int come; // 进入校门的人数
		int go; // 出去校门的人数

		public GateCount(String in, String out) {
			this.inside = in;
			this.outside = out;
			this.come = 0;
			this.go = 0;
		}
	}

	public Connection connDatabase(String user, String passwd)
			throws SQLException, ClassNotFoundException {
		try {
			Class.forName(DBDRIVER);
		} catch (ClassNotFoundException e) {
			throw e;
		}
		String DBUSER = user;
		String DBPASSEORD = passwd;
		conn = DriverManager.getConnection(DBURL, DBUSER, DBPASSEORD);
		return conn;
	}

	public File datafolder; // 存储原始文件的总目录
	public File[] monfolders; // 原始文件总目录下的子目录，即各个监测点的数据文件夹
	public String day;

	public void init(String _day) {
		day = _day;
	}
	
	// torun函数内主要完成功能：计算各个校门的进出人数
		public void torun() {
			long daytime = getgelin(day);
			HashMap<String, GateCount> gates = getgate(daytime);
			String sql1 = "INSERT INTO realgate (time, gateid,alldata) VALUES(?,?,?)";

			// 将活跃度值和进出校门人数的结果插入到数据库中
			try {
				PreparedStatement pre1 = conn.prepareStatement(sql1);
				Iterator<Entry<String, GateCount>> it = gates.entrySet().iterator();
				while (it.hasNext()) {
					Entry<String, GateCount> en = it.next();
					String gateid = en.getKey();
					int go = en.getValue().go;
					int come = en.getValue().come;
					int all=go+come;
					pre1.setLong(1, daytime);
					pre1.setString(2, gateid);
					pre1.setInt(3, all);
					pre1.executeUpdate();
					logger.info("insert into goandcome: time,gateid,alldata "
							+ daytime + " , " + come + " , " + go + "," + gateid + ".");

				}
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				logger.error(e1.getMessage());
			}
		}

	// 获取每个校门的进出人数，inside 代表校内的路由器，outside代表校外的路由器
	public HashMap<String, GateCount> getgate(long daytime) {
		long start = daytime;
		long end = daytime + 5*60;
		String sql1 = "SELECT mac,intime FROM visitrecord WHERE monid = ?  and intime>='"
				+ start + "' and intime<='" + end + "'";
        //测试的时候，由于原始数据限制，先测一个门的
		HashMap<String, GateCount> gatemap = new HashMap<String, GateCount>();
//		gatemap.put("west", new GateCount("westInside", "westOutside"));
//		gatemap.put("east", new GateCount("eastInside", "eastOutside"));
//		gatemap.put("middle", new GateCount("middleInside", "middleOutside"));
//		gatemap.put("north", new GateCount("northInside", "northOutside"));
		
		gatemap.put("west", new GateCount("14E4E6E17648", "14E4E6E176C8"));  //教4
		gatemap.put("east", new GateCount("eastInside", "eastOutside"));
		gatemap.put("middle", new GateCount("14E4E6E14460", "14E6E4E142A8"));//教3
		gatemap.put("north", new GateCount("14E4E6E1738C", "14E4E6E186A4"));//科研楼
        
		try {
			PreparedStatement pre1 = conn.prepareStatement(sql1);
			PreparedStatement pre2 = conn.prepareStatement(sql1);
			// 逐个处理每个校门
			Iterator<Entry<String, GateCount>> gateit = gatemap.entrySet()
					.iterator();
			while (gateit.hasNext()) {
				int comevalue = 0;
				int govalue = 0;
				Entry<String, GateCount> gateentry = gateit.next();
				// 1，获取此门的校内和校外的两个路由器的id
				String inside = gateentry.getValue().inside;
				String outside = gateentry.getValue().outside;

				// 2,从DB的visitrecord中取数据,获取校内和校外的路由的当天的mac与intime
				HashMap<String, ArrayList<Integer>> insidemap = new HashMap<String, ArrayList<Integer>>();
				HashMap<String, ArrayList<Integer>> outsidemap = new HashMap<String, ArrayList<Integer>>();
				pre1.setString(1, inside);
				pre2.setString(1, outside);
				ResultSet rs1 = pre1.executeQuery();
				ResultSet rs2 = pre2.executeQuery();
				while (rs1.next()) {
					String mac = rs1.getString(1);
					Integer intime = rs1.getInt(2);
					if (insidemap.containsKey(mac)) {
						insidemap.get(mac).add(intime);
					} else {
						ArrayList<Integer> intimelist = new ArrayList<Integer>();
						intimelist.add(intime);
						insidemap.put(mac, intimelist);
					}
				}
				while (rs2.next()) {
					String mac = rs2.getString(1);
					Integer intime = rs2.getInt(2);
					if (outsidemap.containsKey(mac)) {
						outsidemap.get(mac).add(intime);
					} else {
						ArrayList<Integer> intimelist = new ArrayList<Integer>();
						intimelist.add(intime);
						outsidemap.put(mac, intimelist);
					}
				}
				// 3,找出校内和校外重合的mac ，并判断其进出
				/*
				 * 判断方法： 校内路由路由器的intime和校外路由器的intime的绝对值不超过300sec
				 * 若校内intime大于校外intime 则判断为进入 若校内intime小于校外intime 则判断为出去
				 */
				Iterator<Entry<String, ArrayList<Integer>>> init = insidemap
						.entrySet().iterator();
				// Iterator<Entry<String,ArrayList<Integer>>> outit =
				// outsidemap.entrySet().iterator();

				while (init.hasNext()) {
					Entry<String, ArrayList<Integer>> entry = init.next();
					String mac = entry.getKey();
					// 找到重合的mac
					if (outsidemap.containsKey(mac)) {
						ArrayList<Integer> inlist = entry.getValue();
						ArrayList<Integer> outlist = outsidemap.get(mac);
						for (int i = 0; i < inlist.size(); i++) {
							int min = 500;
							int anthor = 0;
							for (int j = 0; j < outlist.size(); j++) {
								int diff = Math.abs(inlist.get(i)
										- outlist.get(j));
								// 取在300秒范围内，且时间差最小的一个
								if (diff < 300) {
									if (diff < min) {
										min = diff;
										anthor = outlist.get(j);
									}
								}
							}
							if (anthor != 0) {
								if (inlist.get(i) < anthor) {
									govalue++;
								} else {
									comevalue++;
								}
							}
						}
					}
				}
				gateentry.getValue().come = comevalue;
				gateentry.getValue().go = govalue;
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return gatemap;
	}

	
	// 将字符串转为时间戳
			// 20140712 xx:xx:xx --> 类似1405814899的数字
		 public long getgelin(String user_time) { 
				long re_time = 0; 
				String re_time1 = null; 
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd hh:mm:ss"); 
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
		 
	public static void main(String[] args) {
		try {
			RealGate ti = new RealGate();
			String user = args[0]; // db username
			String passwd = args[1]; // db passwd
			String day=args[2];// day,"20140525"
			String time=args[3];  //time,"12:20:34"
			String daytime=day+" "+time;
			conn = ti.connDatabase(user, passwd);
			ti.init(daytime);
			ti.torun();
		} catch (Exception e) {
			logger.error("error!!!" + e.getMessage());
			e.printStackTrace();
		} finally {
			logger.info("*******ENTER finally handler*******");
			if (conn != null)
				try {
					conn.close();
				} catch (SQLException e1) {
					logger.error(e1.getMessage());
				}
		}
	}
}

