package realgate;
/*�����ܣ�ͳ�Ƶ���У԰��Ծ�ȣ������뵽DB�б�activity��
 *       ͳ�Ƶ��춫���С��������ĸ���ÿ��Ľ���˴Σ������뵽DB�б�goandcome��
 *��������Ƶ�ʣ�ÿ5����һ��
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

	public static final String DBDRIVER = "com.mysql.jdbc.Driver"; // ������ݿ������
	public static final String DBURL = "jdbc:mysql://localhost:3306/wibupt"; // ������ݿ����ӵ�ַ
	private static Connection conn = null; // ������̬��Connection����

	public class GateCount {
		String inside; // ����У�ڵ�·������id
		String outside; // ����У���·������id
		int come; // ����У�ŵ�����
		int go; // ��ȥУ�ŵ�����

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

	public File datafolder; // �洢ԭʼ�ļ�����Ŀ¼
	public File[] monfolders; // ԭʼ�ļ���Ŀ¼�µ���Ŀ¼�����������������ļ���
	public String day;

	public void init(String _day) {
		day = _day;
	}
	
	// torun��������Ҫ��ɹ��ܣ��������У�ŵĽ������
	public void torun() {
		long daytime = getgelin(day);
		HashMap<String, GateCount> gates = getgate(daytime);
		String sql1 = "INSERT INTO realgate (time, gateid,alldata) VALUES(?,?,?)";

		// ����Ծ��ֵ�ͽ��У������Ľ����뵽��ݿ���
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
				logger.info("insert into realgate: time,gateid,alldata "
						+ daytime + " , " + gateid + " , " + all + ".");

			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			logger.error(e1.getMessage());
		}
	}

	// ��ȡÿ��У�ŵĽ������inside ���У�ڵ�·������outside���У���·����
	public HashMap<String, GateCount> getgate(long daytime) {
		long start = daytime - 5 * 60;
		long end = daytime;
		String sql1 = "SELECT mac,inTime FROM (SELECT * FROM visitrecord ORDER BY id DESC LIMIT 200000) AS subvisitrecord" +
				" WHERE monid = ? AND inTime BETWEEN ? AND ?";
        //���Ե�ʱ������ԭʼ������ƣ��Ȳ�һ���ŵ�
		HashMap<String, GateCount> gatemap = new HashMap<String, GateCount>();
//		gatemap.put("west", new GateCount("westInside", "westOutside"));
//		gatemap.put("east", new GateCount("eastInside", "eastOutside"));
//		gatemap.put("middle", new GateCount("middleInside", "middleOutside"));
//		gatemap.put("north", new GateCount("northInside", "northOutside"));
		
		String[] westMacs = {"14E4E6E17648", "14E4E6E176C8"}; // teaching building 4
		String[] eastMacs = {"0C8268F9314E", "0C8268F15C64"}; // student apartment 29
		String[] middleMacs = {"388345A236BE", "5C63BFD90AE2"}; // teaching building 3
		String[] northMacs = {"0C8268C7D504", "0C8268F90E64"}; // student apartment 10 south entrance
		
		gatemap.put("west", new GateCount(westMacs[0], westMacs[1]));  
		gatemap.put("east", new GateCount(eastMacs[0], eastMacs[1]));
		gatemap.put("middle", new GateCount(middleMacs[0], middleMacs[1]));
		gatemap.put("north", new GateCount(northMacs[0], northMacs[1]));
        
		try {
			PreparedStatement pre1 = conn.prepareStatement(sql1);
			PreparedStatement pre2 = conn.prepareStatement(sql1);
			// �������ÿ��У��
			Iterator<Entry<String, GateCount>> gateit = gatemap.entrySet()
					.iterator();
			while (gateit.hasNext()) {
				int comevalue = 0;
				int govalue = 0;
				Entry<String, GateCount> gateentry = gateit.next();
				// 1����ȡ���ŵ�У�ں�У�������·������id
				String inside = gateentry.getValue().inside;
				String outside = gateentry.getValue().outside;
				// 2,��DB��visitrecord��ȡ���,��ȡУ�ں�У���·�ɵĵ����mac��intime
				HashMap<String, ArrayList<Integer>> insidemap = new HashMap<String, ArrayList<Integer>>();
				HashMap<String, ArrayList<Integer>> outsidemap = new HashMap<String, ArrayList<Integer>>();
				pre1.setString(1, inside);
				pre1.setLong(2, start);
				pre1.setLong(3, end);
				ResultSet rs1 = pre1.executeQuery();
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
				pre2.setString(1, outside);
				pre2.setLong(2, start);
				pre2.setLong(3, end);
				ResultSet rs2 = pre2.executeQuery();
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
				// 3,�ҳ�У�ں�У���غϵ�mac �����ж�����
				/*
				 * �жϷ����� У��·��·������intime��У��·������intime�ľ��ֵ������300sec
				 * ��У��intime����У��intime ���ж�Ϊ���� ��У��intimeС��У��intime ���ж�Ϊ��ȥ
				 */
				Iterator<Entry<String, ArrayList<Integer>>> init = insidemap
						.entrySet().iterator();
				// Iterator<Entry<String,ArrayList<Integer>>> outit =
				// outsidemap.entrySet().iterator();
				while (init.hasNext()) {
					Entry<String, ArrayList<Integer>> entry = init.next();
					String mac = entry.getKey();
					// �ҵ��غϵ�mac
					if (outsidemap.containsKey(mac)) {
						ArrayList<Integer> inlist = entry.getValue();
						ArrayList<Integer> outlist = outsidemap.get(mac);
						for (int i = 0; i < inlist.size(); i++) {
							int min = 500;
							int anthor = 0;
							for (int j = 0; j < outlist.size(); j++) {
								int diff = Math.abs(inlist.get(i)
										- outlist.get(j));
								// ȡ��300�뷶Χ�ڣ���ʱ�����С��һ��
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

	
	// ���ַ�תΪʱ���
			// 20140712 xx:xx:xx --> ����1405814899������
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
		logger.info("RealGate begin...");
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

