package brandStat;

/*程序功能：统计当天的校园内所有手机的品牌分布，将结果插入到DB中的表branddis
 * 
 * 程序运行频率：每天一次
 * */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BrandStat{	
	private static Logger logger = LogManager.getLogger(BrandStat.class.getName());
		
	public static final String DBDRIVER   ="com.mysql.jdbc.Driver";		
	public static final String DBURL      ="jdbc:mysql://localhost:3306/wibupt";
	private static Connection conn        = null;
	
	public class BrandInfo {
		public int count = 0;
		public int apple = 0;	public int samsung = 0;	public int nokia = 0;
		public int sony = 0;	public int zte = 0;		public int huawei = 0;
		public int asus = 0;	public int  intel = 0;	public int honhai = 0;
		public int htc = 0;		public int xiaomi = 0;	public int oppo = 0;
		public int lg = 0; 		public int lenovo = 0;	public int meizu = 0;
		public int coolpad = 0;	public int bbk = 0;		public int tp_link = 0;
		public int gionee = 0;	public int murata = 0;	public int inpro = 0;
		public int aw = 0;		public int liteon = 0;	public int arris = 0;
		public int K_Touch = 0;	public int AcSiP = 0;	public int AsiaPacific = 0;
		public int ChiMei = 0;	public int Foxconn = 0;	public int Garmin = 0;
		public int Gemtek = 0;	public int MediaTek = 0;public int Qualcomm = 0;
		public int Hisense = 0;	public int Roving = 0;	public int Simcom = 0;
		public int SHARP = 0;	public int Wisol = 0;	public int Wistron = 0;
		public int Amoi = 0;	public int BIRD = 0;	public int Philips = 0;
		public int TCL = 0;	    public int vivo = 0;    public int leTv = 0;
		public int other = 0;
	};
	
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
	
	public File datafolder = null; 	//存储原始文件的总目录
	public File[] monfolders = null; 	//原始文件总目录下的子目录，即各个监测点的数据文件夹

	public void torun(String datafilepath, String day) {
		datafolder = new File(datafilepath);
		if (datafolder == null || !(datafolder.exists())){  //判断存储原始数据的目录是否存在
			logger.error("Folder: "+datafilepath+"  not found~~ To QUIT!");
			return;
		}
		
		monfolders = datafolder.listFiles(); // 列出data总目录下的所有监测点的data目录
		if(monfolders == null || monfolders.length == 0) {
			logger.error("No folder found in " + datafolder.getName() + "~~ To QUIT!");
			return;
		}
		
		BrandInfo bi = new BrandInfo();   //所有监测点总共的统计信息
		Set<String> incount  = new HashSet<String>();//所有监测点包含的mac
		
		//逐个取每个监测点的数据
		for (int i = 0; i <monfolders.length; i++) {
    		double[] rss;
			try {
				rss = getrss(monfolders[i].getName());
			} catch (SQLException e) {
				logger.error("SQLException when retrieve RSS of:" + monfolders[i].getName() +"~~ To IGNORE it's data!");
				continue;
			}
			if(rss==null) {
				logger.info("Null returned when retrieve RSS of:" + monfolders[i].getName() +"~~ To IGNORE it's data!");
				continue;
			}
			double rss_in = rss[0];  // 该monid设定的室内信号强度的小值，大于rss_in的信号都认为是室内手机发出的信号
			double rss_out = rss[1]; // 暂时不用
			
			logger.info("Begin to analysis: " + monfolders[i].getName() 
					+ " with rss_in:" + rss_in + " and rss_out:" + rss_out );

			//打开监测点目录下某天文件~~只处理当天的数据
			File dayfile = new File(monfolders[i].getPath()+ System.getProperty("file.separator") + day);
			if ( dayfile==null || !(dayfile.exists()) ){	//判断文件是否存在
				logger.info("The data file " + dayfile.getName() + " not found!~~ To IGNORE it's data!");
				continue;    //若没有当天数据，接着处理下一个监测点的数据
			}

			// 逐行处理
			FileInputStream fis = null;
			Scanner monscan = null;
			try {
				fis = new FileInputStream(dayfile);
			} catch (FileNotFoundException e) {
				logger.error("FileNotFoundException!! " + dayfile.getName() + " ~~ To IGNORE it's data!");
				continue;    //若没有当天数据，接着处理下一个监测点的数据
			}
			monscan = new Scanner(fis);
			
			while (monscan.hasNextLine()){
				String line = monscan.nextLine(); // 读取一行
				Pattern subpat=Pattern.compile("[|]") ;			//创建Pattern实例
				String mac_rss_time[]=subpat.split(line);	
//				int linetime   =Integer.parseInt(mac_rss_time[2]); // time
				int linerss    =Integer.parseInt(mac_rss_time[1]); // rss
				String linemac = mac_rss_time[0];
				
//				allcount.add(linemac);  // 所有MAC
				if(linerss > rss_in) incount.add(linemac); //入店MAC
			}
			monscan.close();
			try {
				if(fis!=null) fis.close();
			} catch (IOException e) {
				logger.error(e.getMessage());
				continue;
			}
    	}
		Iterator<String> it = incount.iterator();
		while(it.hasNext()){
			String mac = it.next().toString();
			statBrandOfMac(mac, bi);
		}
		
		insertBrandinfo2DB(day, bi);
		logger.info(" brand analysis completed!");

	}
	
	public void insertBrandinfo2DB( String _day, BrandInfo _bi) {
		String _day_DateFormat = _day.substring(0,4)+"-"+_day.substring(4,6)+"-"+_day.substring(6,8);// 20140707-->2014-07-07
		int known_count = _bi.count-_bi.other;
		String record =  _day_DateFormat + "|" + _bi.count + "|" + known_count + "|" + _bi.other
				+ "|" + _bi.apple + "|" + _bi.samsung+ "|" + _bi.xiaomi + "|" + _bi.htc + "|" + _bi.huawei + "|" + _bi.murata 
				+ "|" + _bi.intel + "|" + _bi.honhai + "|" + _bi.nokia	+ "|" + _bi.lenovo + "|" + _bi.liteon + "|" + _bi.sony 
				+ "|" + _bi.meizu + "|" + _bi.inpro+ "|" + _bi.lg + "|" + _bi.aw + "|" + _bi.oppo + "|" + _bi.zte + "|" + _bi.bbk 
				+ "|" + _bi.arris + "|" + _bi.coolpad + "|" + _bi.asus + "|" + _bi.gionee + "|" + _bi.tp_link + "|" + _bi.K_Touch 
				+ "|" + _bi.AcSiP + "|" + _bi.AsiaPacific + "|" + _bi.ChiMei + "|" + _bi.Foxconn + "|" + _bi.Garmin + "|" + _bi.Gemtek 
				+ "|" + _bi.MediaTek + "|" + _bi.Qualcomm + "|" + _bi.Hisense + "|" + _bi.Roving + "|" + _bi.Simcom	+ "|" + _bi.SHARP 
				+ "|" + _bi.Wisol + "|" + _bi.Wistron + "|" + _bi.Amoi + "|" + _bi.BIRD + "|" + _bi.Philips+ "|" + _bi.TCL + "|" + _bi.other
				+ "|" + _bi.vivo + "|" + _bi.leTv;	
		PreparedStatement perstat =null;
		String sql="INSERT INTO branddis (time,counter,cKnown,cUnknown,apple,samsung,xiaomi,htc,huawei,murata,intel,honhai,nokia,"
				+"lenovo,liteon,sony,meizu,inpro,lg,AzureWave,oppo,zte,bbk,arris,coolpad,asus,gionee,tp_link,K_Touch,AcSiP,AsiaPacific,"
				+"ChiMei,Foxconn,Garmin,Gemtek,MediaTek,Qualcomm,Hisense,Roving,Simcom,SHARP,Wisol,Wistron,Amoi,BIRD,Philips,TCL,other,vivo,leTv"
				+") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		try{
			Pattern subpat=Pattern.compile("[|]") ;			
			String subrse[]=subpat.split(record);
			perstat = conn.prepareStatement(sql);
		    //perstat.setString(1,subrse[0]);
			perstat.setString(1,subrse[0]);
			for (int i=2;i<=subrse.length;i++){
				perstat.setInt(i,Integer.valueOf(subrse[i-1]));				
			}
			perstat.executeUpdate();
		}catch (SQLException e) {
			logger.error(e.getMessage());
			return;
		}
	}
		
	public void statBrandOfMac(String _mac, BrandInfo _bi) {
		String firsthalfofmac = _mac.substring(0,6);
		//在数据表MacBrand中查询MAC对应的厂商信息，进行品牌分类统计
		String sql2 = "SELECT brand FROM macbrand_oui where mac = '" + firsthalfofmac + "';";			
		PreparedStatement perstat2 =null;
		ResultSet res2 = null;
		String branddescription = null;
		try{
			perstat2 = conn.prepareStatement(sql2);
			res2 = perstat2.executeQuery() ;
			while (res2.next()){
				branddescription = res2.getString(1);
			}					
		} catch (SQLException e) {
			branddescription = null;
		}
		if(branddescription==null) return;
		
		if (branddescription.indexOf("Apple")!=-1||branddescription.indexOf("APPLE")!=-1) { _bi.apple++; }
		else if (branddescription.indexOf("Samsung")!=-1||branddescription.indexOf("SAMSUNG")!=-1) { _bi.samsung++;}
		else if (branddescription.indexOf("Nokia")!=-1||branddescription.indexOf("NOKIA")!=-1) { _bi.other++; _bi.nokia++;}
		else if (branddescription.indexOf("Sony")!=-1||branddescription.indexOf("SONY")!=-1) {_bi.other++;_bi.sony++;}					
		else if (branddescription.indexOf("huawei")!=-1||branddescription.indexOf("HUAWEI")!=-1) {_bi.huawei++;}
		else if (branddescription.indexOf("ZTE")!=-1||branddescription.indexOf("zte")!=-1) {_bi.other++;_bi.zte++;}
		else if (branddescription.indexOf("Intel")!=-1||branddescription.indexOf("INTEL")!=-1) {_bi.other++;_bi.intel++;}
		else if (branddescription.indexOf("Hon Hai")!=-1||branddescription.indexOf("HON HAI")!=-1) {_bi.other++;_bi.honhai++;}
		else if (branddescription.indexOf("ASUS")!=-1||branddescription.indexOf("Asus")!=-1) {_bi.other++;_bi.asus++;}
		else if (branddescription.indexOf("HTC")!=-1) {_bi.other++;_bi.htc++;}
		else if (branddescription.indexOf("OPPO") != -1) {_bi.oppo++;}
		else if (branddescription.indexOf("XIAOMI")!=-1||branddescription.indexOf("Xiaomi")!=-1) {_bi.xiaomi++;}
		else if (branddescription.indexOf("MEIZU")!=-1) {_bi.meizu++;}
		else if (branddescription.indexOf("LG")!=-1) {_bi.other++;_bi.lg++;}
		else if (branddescription.indexOf("Lenovo Mobile")!=-1 || branddescription.indexOf("Motorola Mobility") != -1) {_bi.other++;_bi.lenovo++;}
		else if (branddescription.indexOf("Yulong")!=-1) {_bi.coolpad++;}
		else if (branddescription.indexOf("BBK")!=-1) {_bi.other++;_bi.bbk++;}
		else if (branddescription.indexOf("TP-LINK")!=-1) {_bi.other++;_bi.tp_link++;}
		else if (branddescription.indexOf("Gionee")!=-1) {_bi.gionee++;}
		else if (branddescription.indexOf("Murata")!=-1) {_bi.other++;_bi.murata++;}
		else if (branddescription.indexOf("InPro")!=-1) {_bi.other++;_bi.inpro++;}
		else if (branddescription.indexOf("Liteon")!=-1) {_bi.other++;_bi.liteon++;}
		else if (branddescription.indexOf("AzureWave")!=-1) {_bi.other++;_bi.aw++;}
		else if (branddescription.indexOf("ARRIS")!=-1) {_bi.other++;_bi.arris++;}
		else if (branddescription.indexOf("AcSiP")!=-1) {_bi.other++;_bi.AcSiP++;}
		else if (branddescription.indexOf("Asia Pacelse ific")!=-1) {_bi.other++;_bi.AsiaPacific++;}
		else if (branddescription.indexOf("BenyWave")!=-1||branddescription.indexOf("Beny Wave")!=-1) {_bi.other++;}
		else if (branddescription.indexOf("Chi Mei")!=-1) {_bi.other++;_bi.ChiMei++;}
		else if (branddescription.indexOf("Foxconn")!=-1||branddescription.indexOf("FOXCONN")!=-1) {_bi.other++;_bi.Foxconn++;}
		else if (branddescription.indexOf("Garmin")!=-1) {_bi.other++;_bi.Garmin++;}
		else if (branddescription.indexOf("Gemtek")!=-1) {_bi.other++;_bi.Gemtek++;}
		else if (branddescription.indexOf("MediaTek")!=-1) {_bi.other++;_bi.MediaTek++;}
		else if (branddescription.indexOf("Qualcomm")!=-1||branddescription.indexOf("QUALCOMM")!=-1||branddescription.indexOf("QCOM")!=-1) {_bi.other++;_bi.Qualcomm++;}
		else if (branddescription.indexOf("Hisense")!=-1||branddescription.indexOf("HISENSE")!=-1) {_bi.other++;_bi.Hisense++;}
		else if (branddescription.indexOf("Roving")!=-1) {_bi.other++;_bi.Roving++;}
		else if (branddescription.indexOf("Simcom")!=-1) {_bi.other++;_bi.Simcom++;}
		else if (branddescription.indexOf("SHARP")!=-1) {_bi.other++;_bi.SHARP++;}
		else if (branddescription.indexOf("WISOL")!=-1||branddescription.indexOf("Wisol")!=-1) {_bi.other++;_bi.Wisol++;}
		else if (branddescription.indexOf("Wistron")!=-1) {_bi.other++;_bi.Wistron++;}
		else if (branddescription.indexOf("AMOI ELECTRONICS")!=-1) {_bi.other++;_bi.Amoi++;}
		else if (branddescription.indexOf("Bird Electronic")!=-1) {_bi.other++;_bi.BIRD++;}
		else if (branddescription.indexOf("PHILIPS")!=-1||branddescription.indexOf("Philips")!=-1) {_bi.other++;_bi.Philips++;}
		else if (branddescription.indexOf("TCL ")!=-1) {_bi.other++;_bi.TCL++;}
		else if (branddescription.indexOf("vivo Mobile") != -1) {_bi.vivo++;}
		else if (branddescription.indexOf("Letv") != -1) {_bi.leTv++;}
		else {_bi.other++;}
		_bi.count++;
	}

	public double[] getrss(String monid) throws SQLException {
		double[] rss= new double[2];
		PreparedStatement perstat =null;
		ResultSet res=null;
		String sql="select rssin,rssout FROM monindex where monid='"+monid+"';";
		perstat = conn.prepareStatement(sql); //实例化Statement对象
		res = perstat.executeQuery() ;
		while (res.next()){	
			for (int i=0;i<2;i++)
			{
				rss[i]=res.getInt(i+1);
			}
		}
		return rss;
	}

	public String getYestoday() {
		long time=System.currentTimeMillis();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String today = sdf.format(new Date(time-24*60*60*1000L)); 
		return today; 
	} 
	
	public static void main(String[] args) {			
		try{
			BrandStat  brandstat =new BrandStat();
			String user=args[0];//username,eg."root"
			String passwd=args[1];//password,eg."admin"
			String dataPath=args[2];//dataPath,eg."D:\\data"
			String day;//day,"20140525"
			if (args[3].equals("yesterday"))
			{
				day=brandstat.getYestoday();
			}
			else {
				day=args[3];
			}
			conn =brandstat.connDatabase(user,passwd);
			brandstat.torun(dataPath,day); 
		}catch (Exception e){
			logger.error(e.getMessage());
			logger.error("error!!!");
		}finally{
			logger.info("*******ENTER finally handler*******");
			if(conn!=null)
				try {
					conn.close();
				} catch (SQLException e1) {
					logger.error(e1.getMessage());
				}
		}
	}
}

