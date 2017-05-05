// Decompiled by Jad v1.5.8e2. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://kpdus.tripod.com/jad.html
// Decompiler options: packimports(3) fieldsfirst ansi space 
// Source File Name:   TotalInfo.java

package totalinfo;

import java.io.*;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TotalInfo
{
	public class GateCount
	{

		String inside;
		String outside;
		int come;
		int go;

		public GateCount(String in, String out)
		{
			this.inside = in;
			this.outside = out;
			this.come = 0;
			this.go = 0;
		}
	}


	private static Logger logger = LogManager.getLogger(TotalInfo.class
			.getName());	
	public static final String DBDRIVER = "com.mysql.jdbc.Driver";
	public static final String DBURL = "jdbc:mysql://localhost:3306/wibupt";
	private static Connection conn = null;
	public File datafolder;
	public File monfolders[];
	public String datafilepath;
	public String day;

	public TotalInfo()
	{
	}

	public Connection connDatabase(String user, String passwd)
		throws SQLException, ClassNotFoundException
	{
		try
		{
			Class.forName("com.mysql.jdbc.Driver");
		}
		catch (ClassNotFoundException e)
		{
			throw e;
		}
		String DBUSER = user;
		String DBPASSEORD = passwd;
		conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/wibupt", DBUSER, DBPASSEORD);
		return conn;
	}

	public void init(String _datapath, String _day)
	{
		datafilepath = _datapath;
		day = _day;
	}

	public void torun()
	{
		HashSet groupids = new HashSet();
		try
		{
			String sql1 = "select groupid from groupindex";
			Statement st = conn.createStatement();
			for (ResultSet rst = st.executeQuery(sql1); rst.next(); groupids.add(rst.getString(1)));
		}
		catch (SQLException e1)
		{
			e1.printStackTrace();
		}
		String sql2 = "select monid from monindex where groupid = ?";
		try
		{
			PreparedStatement pre1 = conn.prepareStatement(sql2);
			HashSet monids;
			String gid;
			for (Iterator it = groupids.iterator(); it.hasNext(); groupcomein(gid, monids, day))
			{
				monids = new HashSet();
				gid = (String)it.next();
				pre1.setString(1, gid);
				for (ResultSet rs = pre1.executeQuery(); rs.next(); monids.add(rs.getString(1)));
			}

		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		datafolder = new File(datafilepath);
		if (datafolder == null || !datafolder.exists())
		{
			logger.error((new StringBuilder("Folder: ")).append(datafilepath).append("  not found~~ To QUIT!").toString());
			return;
		}
		monfolders = datafolder.listFiles();
		if (monfolders == null || monfolders.length == 0)
		{
			logger.error((new StringBuilder("No folder found in ")).append(datafolder.getName()).append("~~ To QUIT!").toString());
			return;
		}
		HashSet allmonMacs = new HashSet();
		double everymonCount = 0.0D;
		int everymonCountperhour[] = new int[24];
		for (int i = 0; i < 24; i++)
			everymonCountperhour[i] = 0;

		for (int i = 0; i < monfolders.length; i++)
		{
			double rss[];
			try
			{
				rss = getrss(monfolders[i].getName());
			}
			catch (SQLException e)
			{
				logger.error((new StringBuilder("SQLException when retrieve RSS of:")).append(monfolders[i].getName()).append("~~ To IGNORE it's data!").toString());
				continue;
			}
			if (rss == null)
			{
				logger.error((new StringBuilder("Null returned when retrieve RSS of:")).append(monfolders[i].getName()).append("~~ To IGNORE it's data!").toString());
				continue;
			}
			double rss_in = rss[0];
			double rss_out = rss[1];
			logger.info((new StringBuilder("Begin to analysis: ")).append(monfolders[i].getName()).append(" with rss_in:").append(rss_in).append(" and rss_out:").append(rss_out).append(" in day:").append(day).toString());
			File dayfile = new File((new StringBuilder(String.valueOf(monfolders[i].getPath()))).append(System.getProperty("file.separator")).append(day).toString());
			if (dayfile == null || !dayfile.exists())
			{
				logger.info((new StringBuilder("The data file ")).append(dayfile.getName()).append(" not found!~~ To IGNORE it's data!").toString());
				continue;
			}
			FileInputStream fis = null;
			Scanner monscan = null;
			try
			{
				fis = new FileInputStream(dayfile);
			}
			catch (FileNotFoundException e)
			{
				logger.error((new StringBuilder("FileNotFoundException!! ")).append(dayfile.getName()).append(" ~~ To IGNORE it's data!").toString());
				continue;
			}
			monscan = new Scanner(fis);
			Set incount = new HashSet();
			ArrayList incountperhour = new ArrayList();
			for (int j = 0; j < 24; j++)
				incountperhour.add(new HashSet());

			while (monscan.hasNextLine()) 
			{
				String line = monscan.nextLine();
				Pattern subpat = Pattern.compile("[|]");
				String mac_rss_time[] = subpat.split(line);
				int linerss = Integer.parseInt(mac_rss_time[1]);
				String linemac = mac_rss_time[0];
				String linetime = mac_rss_time[2];
				if ((double)linerss > rss_in)
				{
					incount.add(linemac);
					allmonMacs.add(linemac);
//					linetime = String.valueOf(Integer.parseInt(linetime) + 28800);
					linetime = String.valueOf(Integer.parseInt(linetime));
					int hour = Integer.parseInt(getHour(linetime));
					((Set)incountperhour.get(hour)).add(linemac);
				}
			}
			monscan.close();
			try
			{
				if (fis != null)
					fis.close();
			}
			catch (IOException e)
			{
				logger.error(e.getMessage());
				continue;
			}
			everymonCount += incount.size();
			for (int k = 0; k < 24; k++)
				everymonCountperhour[k] += ((Set)incountperhour.get(k)).size();

		}

		double activity = everymonCount / (double)allmonMacs.size();
		activity = (double)Math.round(activity * 100D) / 100D;
//		long daytime = getgelin(day) - 28800L;
		long daytime = getgelin(day);
		String type = getweek(day);
		HashMap gates = getgate(daytime);
		int allgatego = 0;
		int allgatecome = 0;
		String riqi = (new StringBuilder(String.valueOf(day.substring(0, 4)))).append("-").append(day.substring(4, 6)).append("-").append(day.substring(6, 8)).toString();
		String sql1 = "INSERT INTO goandcome (time, comeIn,goOut,gateId) VALUES(?,?,?,?)";
		try
		{
			Statement st1 = conn.createStatement();
			st1.execute((new StringBuilder("INSERT INTO  activity  VALUES('")).append(type).append("','").append(daytime).append("','").append(activity).append("')").toString());
logger.info("torun - Begin to insert into activityinday " + daytime);
			for (int i = 0; i < 24; i++)
			{
				Statement st0 = conn.createStatement();
				st0.execute((new StringBuilder("REPLACE INTO activityinday VALUES('")).append(daytime).append("','").append(i).append("','").append(everymonCountperhour[i]).append("')").toString());
				daytime += 3600L;
			}
logger.info("torun - Finish to insert into activityinday " + daytime);
			PreparedStatement pre1 = conn.prepareStatement(sql1);
			String gateid;
			int go;
			int come;
			for (Iterator it = gates.entrySet().iterator(); it.hasNext(); logger.info((new StringBuilder("insert into goandcome: time,come,go,gateid ")).append(riqi).append(" , ").append(come).append(" , ").append(go).append(",").append(gateid).append(".").toString()))
			{
				java.util.Map.Entry en = (java.util.Map.Entry)it.next();
				gateid = (String)en.getKey();
				go = ((GateCount)en.getValue()).go;
				allgatego += go;
				come = ((GateCount)en.getValue()).come;
				allgatecome += come;
				pre1.setString(1, riqi);
				pre1.setInt(2, come);
				pre1.setInt(3, go);
				pre1.setString(4, gateid);
				pre1.executeUpdate();
			}

		}
		catch (SQLException e1)
		{
			logger.error(e1.getMessage());
		}
		logger.info((new StringBuilder("insert into activity: time,activity,monid ")).append(daytime).append(",").append(activity).append(".").toString());
		try
		{
			Statement st2 = conn.createStatement();
			String allgateid = "all";
			st2.execute((new StringBuilder("INSERT INTO  goandcome (time, comeIn,goOut,gateId) VALUES('")).append(riqi).append("',").append(allgatecome).append(",").append(allgatego).append(",'").append(allgateid).append("')").toString());
		}
		catch (SQLException e2)
		{
			logger.error(e2.getMessage());
		}
	}

	public void groupcomein(String gid, Set monids, String day)
	{
		File datafolder = new File(datafilepath);
		if (datafolder == null || !datafolder.exists())
		{
			logger.error((new StringBuilder("Folder: ")).append(datafilepath).append("  not found~~ To QUIT!").toString());
			return;
		}
		File monfolders[] = datafolder.listFiles();
		if (monfolders == null || monfolders.length == 0)
		{
			logger.error((new StringBuilder("No folder found in ")).append(datafolder.getName()).append("~~ To QUIT!").toString());
			return;
		}
		Set incount = new HashSet();
		for (int i = 0; i < monfolders.length; i++)
		{
			if (!monids.contains(monfolders[i].getName()))
				continue;
			double rss[];
			try
			{
				rss = getrss(monfolders[i].getName());
			}
			catch (SQLException e)
			{
				logger.error((new StringBuilder("SQLException when retrieve RSS of:")).append(monfolders[i].getName()).append("~~ To IGNORE it's data!").toString());
				continue;
			}
			if (rss == null)
			{
				logger.error((new StringBuilder("Null returned when retrieve RSS of:")).append(monfolders[i].getName()).append("~~ To IGNORE it's data!").toString());
				continue;
			}
			double rss_in = rss[0];
			double rss_out = rss[1];
			logger.info((new StringBuilder("Begin to analysis: ")).append(monfolders[i].getName()).append(" with rss_in:").append(rss_in).append(" and rss_out:").append(rss_out).append(" in day:").append(day).toString());
			File dayfile = new File((new StringBuilder(String.valueOf(monfolders[i].getPath()))).append(System.getProperty("file.separator")).append(day).toString());
			if (dayfile == null || !dayfile.exists())
			{
				logger.info((new StringBuilder("The data file ")).append(dayfile.getName()).append(" not found!~~ To IGNORE it's data!").toString());
				continue;
			}
			FileInputStream fis = null;
			Scanner monscan = null;
			try
			{
				fis = new FileInputStream(dayfile);
			}
			catch (FileNotFoundException e)
			{
				logger.error((new StringBuilder("FileNotFoundException!! ")).append(dayfile.getName()).append(" ~~ To IGNORE it's data!").toString());
				continue;
			}
			for (monscan = new Scanner(fis); monscan.hasNextLine();)
			{
				String line = monscan.nextLine();
				Pattern subpat = Pattern.compile("[|]");
				String mac_rss_time[] = subpat.split(line);
				int linerss = Integer.parseInt(mac_rss_time[1]);
				String linemac = mac_rss_time[0];
				if ((double)linerss > rss_in)
					incount.add(linemac);
			}

			monscan.close();
			try
			{
				if (fis != null)
					fis.close();
			}
			catch (IOException e)
			{
				logger.error(e.getMessage());
			}
		}

		String riqi = (new StringBuilder(String.valueOf(day.substring(0, 4)))).append("-").append(day.substring(4, 6)).append("-").append(day.substring(6, 8)).toString();
		try
		{
			String sql = (new StringBuilder("INSERT INTO totalinfo (time,comein,groupid) VALUES('")).append(riqi).append("', ").append(incount.size()).append(", '").append(gid).append("')").toString();
			Statement st = conn.createStatement();
			st.execute(sql);
		}
		catch (SQLException e)
		{
			logger.error(e.getMessage());
			logger.error((new StringBuilder("@@@ERROR: ")).append(gid).append(" Failed to insert totalinfo to DB!!!").toString());
		}
		logger.info((new StringBuilder("insert into totalinfo: time,comein,groupname ")).append(riqi).append(",").append(incount.size()).append(",").append(gid).append(".").toString());
	}

	public HashMap getgate(long daytime)
	{
		long start = daytime;
		long end = daytime + 0x15180L;
		String sql1 = (new StringBuilder("SELECT mac,intime FROM visitrecord WHERE monid = ?  and intime>='")).append(start).append("' and intime<='").append(end).append("'").toString();
		HashMap gatemap = new HashMap();	
		
		String[] westMacs = {"14E4E6E17648", "14E4E6E176C8"}; // teaching building 4
		String[] eastMacs = {"0C8268F9314E", "0C8268F15C64"}; // student apartment 29
		String[] middleMacs = {"388345A236BE", "5C63BFD90AE2"}; // teaching building 3
		String[] northMacs = {"0C8268C7D504", "0C8268F90E64"}; // student apartment 10 south entrance

		gatemap.put("west", new GateCount(westMacs[0], westMacs[1]));
		gatemap.put("east", new GateCount(eastMacs[0], eastMacs[1]));  
		gatemap.put("middle", new GateCount(middleMacs[0], middleMacs[1]));
		gatemap.put("north", new GateCount(northMacs[0], northMacs[1]));

		try
		{
			PreparedStatement pre1 = conn.prepareStatement(sql1);
			PreparedStatement pre2 = conn.prepareStatement(sql1);
			for (Iterator gateit = gatemap.entrySet().iterator(); gateit.hasNext();)
			{
				int comevalue = 0;
				int govalue = 0;
				java.util.Map.Entry gateentry = (java.util.Map.Entry)gateit.next();
				String inside = ((GateCount)gateentry.getValue()).inside;
				String outside = ((GateCount)gateentry.getValue()).outside;
				HashMap insidemap = new HashMap();
				HashMap outsidemap = new HashMap();
				pre1.setString(1, inside);
				pre2.setString(1, outside);
				ResultSet rs1 = pre1.executeQuery();
				ResultSet rs2 = pre2.executeQuery();
				while (rs1.next()) 
				{
					String mac = rs1.getString(1);
					Integer intime = Integer.valueOf(rs1.getInt(2));
					if (insidemap.containsKey(mac))
					{
						((ArrayList)insidemap.get(mac)).add(intime);
					} else
					{
						ArrayList intimelist = new ArrayList();
						intimelist.add(intime);
						insidemap.put(mac, intimelist);
					}
				}
				while (rs2.next()) 
				{
					String mac = rs2.getString(1);
					Integer intime = Integer.valueOf(rs2.getInt(2));
					if (outsidemap.containsKey(mac))
					{
						((ArrayList)outsidemap.get(mac)).add(intime);
					} else
					{
						ArrayList intimelist = new ArrayList();
						intimelist.add(intime);
						outsidemap.put(mac, intimelist);
					}
				}
				for (Iterator init = insidemap.entrySet().iterator(); init.hasNext();)
				{
					java.util.Map.Entry entry = (java.util.Map.Entry)init.next();
					String mac = (String)entry.getKey();
					if (outsidemap.containsKey(mac))
					{
						ArrayList inlist = (ArrayList)entry.getValue();
						ArrayList outlist = (ArrayList)outsidemap.get(mac);
						for (int i = 0; i < inlist.size(); i++)
						{
							int min = 500;
							int anthor = 0;
							for (int j = 0; j < outlist.size(); j++)
							{
								int diff = Math.abs(((Integer)inlist.get(i)).intValue() - ((Integer)outlist.get(j)).intValue());
								if (diff < 300 && diff < min)
								{
									min = diff;
									anthor = ((Integer)outlist.get(j)).intValue();
								}
							}

							if (anthor != 0)
								if (((Integer)inlist.get(i)).intValue() < anthor)
									govalue++;
								else
									comevalue++;
						}

					}
				}

				((GateCount)gateentry.getValue()).come = comevalue;
				((GateCount)gateentry.getValue()).go = govalue;
			}

		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		return gatemap;
	}

	public double[] getrss(String monid)
		throws SQLException
	{
		double rss[] = new double[2];
		PreparedStatement perstat = null;
		ResultSet res = null;
		String sql = (new StringBuilder("select rssin,rssout FROM monindex where monid='")).append(monid).append("';").toString();
		perstat = conn.prepareStatement(sql);
		for (res = perstat.executeQuery(); res.next();)
		{
			for (int i = 0; i < 2; i++)
				rss[i] = res.getInt(i + 1);

		}

		return rss;
	}

	public long getgelin(String user_time)
	{
        logger.info("begin to getgelin:" + user_time);
		TimeZone.setDefault(TimeZone.getTimeZone("GMT+8:00"));
		long re_time = 0L;
		String re_time1 = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Date d;
		try
		{
		    d = sdf.parse(user_time);
			long l = d.getTime();
			String str = String.valueOf(l);
			re_time1 = str.substring(0, 10);
			re_time = Long.parseLong(re_time1);
		}
		catch (ParseException e)
		{
			logger.error(e.getMessage());
		}
logger.info("end to getgelin" + re_time);
		return re_time;
	}

	public String getweek(String user_time)
	{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String type = null;
		try
		{
			Date d = sdf.parse(user_time);
			Calendar c = Calendar.getInstance();
			c.setTime(d);
			int w = c.get(7);
			if (w == 1)
				type = "sunday";
			if (w == 2)
				type = "monday";
			if (w == 3)
				type = "tuesday";
			if (w == 4)
				type = "wednesday";
			if (w == 5)
				type = "thursday";
			if (w == 6)
				type = "friday";
			if (w == 7)
				type = "saturday";
		}
		catch (ParseException e)
		{
			logger.error(e.getMessage());
		}
		return type;
	}

	public static String getHour(String beginDate)
	{
		beginDate = (new StringBuilder(String.valueOf(beginDate))).append("000").toString();
		SimpleDateFormat sdf = new SimpleDateFormat("H");
		String sd = sdf.format(new Date(Long.parseLong(beginDate)));
		return sd;
	}

	public String getYestoday()
	{
		long time = System.currentTimeMillis();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String today = sdf.format(new Date(time - 0x5265c00L));
		return today;
	}

	public static void main(String args[])
	{
		try {
			TotalInfo ti = new TotalInfo();
			String user = args[0]; // db username
			String passwd = args[1]; // db passwd
			String datapath = args[2];// dataPath,eg."D:\\data"
			String day;// day,"20140525"
			if (args[3].equals("yesterday")) { // yesterday��ʾֻ��������ģ�20140711��ʾֻ�����������
				day = ti.getYestoday();
			} else {
				day = args[3];
			}
			conn = ti.connDatabase(user, passwd);
			ti.init(datapath, day);
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
