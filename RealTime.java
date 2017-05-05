package realtime;

//ʵʱ������ ��DB �е����ݸ�ʽΪ��monid, time, traffic
//���磬5C63BF7676FC, 1400640300, 5����ʶ1400640300֮ǰ300���ڸü����������Ϊ5

//import java.util.Iterator;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import sun.misc.Signal;
import sun.misc.SignalHandler;

public class RealTime {
	private static Logger logger = LogManager.getLogger(RealTime.class
			.getName());

	public static final String DBDRIVER = "com.mysql.jdbc.Driver"; // �������ݿ���������
	public static final String DBURL = "jdbc:mysql://localhost:3306/wibupt"; // �������ݿ����ӵ�ַ
	public static Connection conn = null; // ������̬��Connection����

	/*
	 * �������ݿ�
	 */
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

	/*
	 * ʹ��PreparedStatementʵ��ִ��SQL��䣬�����ݵ������ݿ�
	 */
	//public File datafolder; // �洢ԭʼ�ļ�����Ŀ¼
	//public File[] monfolders; // ԭʼ�ļ���Ŀ¼�µ���Ŀ¼������������������ļ���
	public int statinterval; // ͳ�Ƽ����ͨ��Ϊÿ5���ӣ�300�룩����һ��ʵʱ����������
	public String datafilepath; // �洢ԭʼ�ļ���·�����ַ���
	public String datetodeal;

	public void init(String _datapath, int _datainterval,
			String _date) {
		datafilepath = _datapath; // ������м���ԭʼ�����ļ�����Ŀ¼
		statinterval = _datainterval; // ÿ�� statinterval
										// ���ʱ�䣬����һ���������ݣ�statinterval���ʱ���ڵ�MAC��ַ����
		datetodeal = _date; // today or yyyyMMdd, like 20140702
	}

	public void torun() {
		
		String day = null;
		if (datetodeal.equals("today"))
			day = getTodayStr();
		else
			day = datetodeal;

		// 1,���ȴӱ�groupindex�����ҳ�����,��idΪŦ��������nameȥ�ң�������������nameΪ�����������
		HashSet<String> groupids = new HashSet<String>();
		try {
			String sql1 = "select groupid from groupindex";
			Statement st = conn.createStatement();
			ResultSet rst = st.executeQuery(sql1);
			while (rst.next()) {
				groupids.add(rst.getString(1));
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		// 2,�ӱ�monindex���ҳ�ÿ��groupid��Ӧ�����м���,������Щ������ܵ�ʵʱ��������������뵽��realtimedata_in��
		String sql2 = "select monid from monindex where groupid = ?";
		
		try {
			PreparedStatement pre1 = conn.prepareStatement(sql2);
			Iterator<String> it = groupids.iterator();
			while (it.hasNext()) {
				HashSet<String> monids = new HashSet<String>();
				String gid = it.next();
				pre1.setString(1, gid);
				ResultSet rs = pre1.executeQuery();
              while(rs.next()){
              	String mon =rs.getString(1);
              	monids.add(mon);
              	logger.info("****get***"+mon+"***from***"+gid);//debug
              }
              getgroupcount(gid,monids,day);
           
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void getgroupcount(String gid,Set<String> monids,String day) {
		
		File datafolder = new File(datafilepath);
		if (datafolder == null || !(datafolder.exists())) { // �жϴ洢ԭʼ���ݵ�Ŀ¼�Ƿ����
			logger.error("Folder: " + datafilepath
					+ "  not found~~ To WAIT the next cycle!");
			return;
		}
		File[] monfolders = datafolder.listFiles();
		if (monfolders == null || monfolders.length == 0) {
			logger.error("No folder found in " + datafolder.getName()
					+ "~~ To QUIT!");
			return;
		}
		
		Map<Integer, HashSet<String>> map_in = new TreeMap<Integer, HashSet<String>>();//��¼�˷����µ�ʵʱ����
			
	
		for(int i=0;i<monfolders.length;i++){
		
				
			//ֻ�д˼������ڴ˷����ʱ��Ŵ�����
			if(monids.contains(monfolders[i].getName())){
				int lasttime_in = getLastTime(gid,"realtimedata_in");
				logger.info(" gruopid " + gid
							+ " last record time in/all:" + lasttime_in );
				if (!datetodeal.equals("today")) {
						// lasttime_in lasttime_all ��Ϊ����0ʱ0�̵ĸ�������ʱ�䣨�룩
						lasttime_in = getgelin(datetodeal);
					//	lasttime_all = lasttime_in;
				}
				double rss[];
				try {
					rss = getrss(monfolders[i].getName());
				} catch (SQLException e) {
					logger.error("SQLException when retrieve RSS of:"
							+ monfolders[i].getName() + "~~ To IGNORE it's data!");
					continue;
				}
				if (rss == null) {
					logger.error("Null returned when retrieve RSS of:"
							+ monfolders[i].getName() + "~~ To IGNORE it's data!");
					continue;
				}
				double rss_in = rss[0]; // ��monid�趨�������ź�ǿ�ȵ�Сֵ������rss_in���źŶ���Ϊ�������ֻ��������ź�
				double rss_out = rss[1]; // �ܱ��ź�ǿ�ȵ�Сֵ������rss_out���źŶ���Ϊ�������ֻ��������ź�
				logger.info("Begin to analysis data : " + monfolders[i].getName()
						+ " .rss_in:" + rss_in + "dBm. rss_out:" + rss_out + "dBm");
				// �򿪼���Ŀ¼��ĳ���ļ�~~ֻ�����������
				File dayfile = new File(monfolders[i].getPath()
						+ System.getProperty("file.separator") + day);
				if (dayfile == null || !(dayfile.exists())) { // �ж��ļ��Ƿ����
					logger.info("The data file " + dayfile.getName()
							+ " not found!~~ To IGNORE it's data!");
					continue; // ��û�е������ݣ����Ŵ�����һ�����������
				}
				// ���д���
				FileInputStream fis = null;
				Scanner monscan = null;
		        try{
					HashSet<String> nexttimeset_in = new HashSet<String>();
	
					fis = new FileInputStream(dayfile);
					monscan = new Scanner(fis);
					while (monscan.hasNextLine()) {
						String line = monscan.nextLine();
						Pattern subpat = Pattern.compile("[|]"); // ����Patternʵ��
						String mac_rss_time[] = subpat.split(line);
						int linetime = Integer.parseInt(mac_rss_time[2]); // time
						int linerss = Integer.parseInt(mac_rss_time[1]); // rss
						String linemac = mac_rss_time[0];
								// ֻ����ʱ��ֵ����(���ڱ����һ����¼��ʱ�� )���ź�ǿ�ȴ��� rss_in ������
						if ((linetime >= lasttime_in) && (linerss > rss_in)) {
							while (linetime >= lasttime_in + statinterval) { // ͨ��ѭ��ʵ���˲���Ĺ���
								if(map_in.containsKey(lasttime_in + statinterval)){
									HashSet<String> temp = new HashSet<String>(map_in.get(lasttime_in + statinterval));
											temp.addAll(nexttimeset_in);
									map_in.put(lasttime_in + statinterval, temp);
								}else{
									HashSet<String> xin = new HashSet<String>(nexttimeset_in);
									map_in.put(lasttime_in + statinterval, xin);
								}
				
								nexttimeset_in.clear();
								lasttime_in = lasttime_in + statinterval;
							}
							nexttimeset_in.add(linemac);
						}
					}
		    }catch (IOException e) {
				logger.error(e.getMessage());
				continue;
			} finally {
				if (fis != null) {
					try {
						fis.close();
					} catch (IOException e) {
						logger.error(e.getMessage());
					}
				}
				if (monscan != null) {
					monscan.close();
				}
			}
		  }
	   }
		//�����������������ݿ�
		try{
			conn.setAutoCommit(false);
			java.sql.Statement stmt = conn.createStatement(
					ResultSet.TYPE_SCROLL_SENSITIVE,
					ResultSet.CONCUR_READ_ONLY);
		
			Iterator<Entry<Integer, HashSet<String>>> iter_in = map_in.entrySet()
					.iterator();
			while (iter_in.hasNext()) {
				Entry<Integer, HashSet<String>> entry = iter_in.next();
				Integer key = (Integer) entry.getKey();
				Integer val = (Integer) entry.getValue().size();
				stmt.execute("INSERT INTO realtimedata_in(monTime,traffic,groupid) "
						+ "VALUES("
						+ key.intValue()
						+ ","
						+ val.intValue()
						+ ",'" + gid + "')");
				stmt.execute("INSERT INTO heatmap(monTime,cnt,groupid) "
						+ "VALUES("
						+ key.intValue()
						+ ","
						+ val.intValue()
						+ ",'" + gid + "')");

			}
			logger.info("To insert " + map_in.size()
					+ " records into realtimedata_in!");
			conn.commit();
		
			
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}	
			
			
	}

	public double[] getrss(String monid) throws SQLException {
		double[] rss = new double[2];
		PreparedStatement perstat = null; // ����PreparedStatement����
		ResultSet res = null;
		String sql = "select rssin,rssout FROM monindex where monid='" + monid
				+ "';";
		try {
			perstat = conn.prepareStatement(sql); // ʵ����Statement����
			res = perstat.executeQuery();
			while (res.next()) {
				for (int i = 0; i < 2; i++) {
					rss[i] = res.getInt(i + 1);
				}
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
			return null;
		}
		return rss;
	}

	public String getTodayStr() {
		long time = System.currentTimeMillis();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String today = sdf.format(new Date(time));
		return today; // 20140718
	}

	// ��ȡ���ݱ��иü������һ�����ݵ�time
	public int getLastTime(String groupid, String tablename) {
		int lastTime = 1399998000; // С�����ʱ������ݶ����Զ���
		PreparedStatement perstat = null; // ����PreparedStatement����
		String sql = "select MAX(monTime) from " + tablename
				+ " where groupid = '" + groupid + "';";
		ResultSet rs = null;
		try {
			perstat = conn.prepareStatement(sql); // ʵ����Statement����
			rs = perstat.executeQuery();
			if (rs.next()) { // ָ�������ƶ�
				lastTime = rs.getInt(1);
				logger.info("******resultset******lasttime*****"+lastTime);//debug
			} else {
				lastTime = (int) getgelin(getTodayStr());
				logger.info("****today*****lasttime*****"+lastTime);//debug
			}
			if (rs != null)
				rs.close();
		} catch (SQLException e) {
			lastTime = (int) getgelin(getTodayStr());
			logger.error(e.getMessage());
		}
		if(lastTime==0){
			lastTime = (int) getgelin(getTodayStr());
			logger.info("****diyiciwei0dezhuanhuan*****lasttime*****"+lastTime);
		}
		logger.info("zuihou****lasttime*****lasttime"+lastTime);//debug
/*
		if (lastTime < 1399998000)
			lastTime = 1399998000;*/

		return lastTime;
	}

	public int getFileSize(String str) {
		int count = 0;
		File f = new File(str);
		try {
			BufferedReader br = new BufferedReader(new FileReader(f));
			while (br.ready()) {
				br.readLine();
				count++;
			}
			br.close();
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		return count;
	}

	// ��ȡ�Ѿ��������ݿ���������ļ��е�λ��
	public int getLastDataPos(int lastTime, String[] str) {
		int lastDataPos = 0;
		for (int i = str.length - 1; i >= 0; i--) {
			Pattern subpat = Pattern.compile("[|]"); // ����Patternʵ��
			String subrse[] = subpat.split(str[i]);
			// System.out.println("i:"+i+"\ttime"+subrse[2]);
			if (Integer.parseInt(subrse[2]) < lastTime) {
				lastDataPos = i + 1;
				break;
			}
		}
		return lastDataPos;
	}

	// ���ַ���תΪʱ���
	// 20140712 --> ����1405814899������
	public int getgelin(String user_time) {
		int re_time = 0;
		String re_time1 = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Date d;
		try {
			d = sdf.parse(user_time);
			long l = d.getTime();
			String str = String.valueOf(l);
			re_time1 = str.substring(0, 10);
			re_time = Integer.parseInt(re_time1);
		} catch (ParseException e) {
			logger.error(e.getMessage());
		}
		return re_time;
	}

	public void reGenerateVRDdata() {
		long daytime = getgelin(datetodeal);
		long daytimeafter24hours = daytime + 24 * 3600;
		PreparedStatement perstat = null;
		String in_sql = "delete from realtimedata_in where monTime > '"
				+ daytime + "' and monTime <'" + daytimeafter24hours + "' ;";
		/*String all_sql = "delete from realtimedata_in    where monTime > '"
				+ daytime + "' and monTime <'" + daytimeafter24hours + "' ;";*/
		try {
			conn.setAutoCommit(false);
			perstat = conn.prepareStatement(in_sql);
			perstat.executeUpdate();
			//perstat = conn.prepareStatement(all_sql);
			perstat.executeUpdate();
			conn.commit();
			logger.info("Firstly, to clear " + datetodeal
					+ " data in realtimedata_in and realtimedata");
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}

		torun();
	}

	public static void main(String[] args) {
		logger.info("realtime begin...");
		SignalHandler handler = new SignalHandler() {
			public void handle(Signal signal) {
				logger.error("capture close signal - " + signal.getName());
				if (conn != null)
					try {
						conn.close();
					} catch (SQLException e) {
						logger.error(e.getMessage());
					}
				System.exit(0);
			}
		};
		Signal.handle(new Signal("TERM"), handler);// �൱��kill -15
		Signal.handle(new Signal("INT"), handler);// �൱��Ctrl+C

		String dbusername = args[0];
		String dbpasswd = args[1];
		String datapath = args[2];
		int datainterval = Integer.parseInt(args[3]);
//		int scaninterval = Integer.parseInt(args[4]);
		String __date = args[4];

		logger.info("Your input information is:");
		logger.info("    Mysql user: " + args[0]);
		logger.info("    Mysql passwd: " + args[1]);
		logger.info("    datapath: " + args[2]);
		logger.info("    data interval: " + args[3] + " seconds");
//		logger.info("    scan interval: " + args[4] + " seconds");
		logger.info("    date: " + args[4]); // today or like 20140712(to delete
												// all visitrecord data of that
												// day and regenerate it again)
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			RealTime rt = new RealTime();
			conn = rt.connDatabase(dbusername, dbpasswd); // ʵ����Connection����
			if (conn == null) {
				logger.error("Failed to get connection from mysql");
				System.exit(0);
			}
			logger.info("Database connection successful!!  "
					+ df.format(new Date()));

			rt.init(datapath, datainterval, __date);
			if (__date.equals("today")) {
				logger.info("Init finished!!! Begin to analysis data on "
						+ df.format(new Date()));
//				while (true) {
					rt.torun();
//					try {
//						logger.info("NOW is " + df.format(new Date())
//								+ " @@@To sleep " + scaninterval
//								+ " seconds...");
//						Thread.sleep(scaninterval * 1000); // ÿ�� scaninterval
//															// �룬����1��
//					} catch (InterruptedException e) {
//						logger.error(e.getMessage());
//					}
//				}
			} else {
				logger.info("Init finished!!! Begin to analysis data on "
						+ __date);
				rt.reGenerateVRDdata();
			}

		} catch (SQLException e) {
			logger.error(e.getMessage());
		} catch (ClassNotFoundException e) {
			logger.error(e.getMessage());
		} finally {
			logger.info("********FINALLY{} handling********");
			if (conn != null)
				try {
					conn.close();
				} catch (SQLException e1) {
					logger.error(e1.getMessage());
				}
		}
	}
}

