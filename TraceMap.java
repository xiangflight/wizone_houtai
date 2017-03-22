
import java.awt.Font;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gephi.data.attributes.api.AttributeColumn;
import org.gephi.data.attributes.api.AttributeController;
import org.gephi.data.attributes.api.AttributeModel;
import org.gephi.graph.api.DirectedGraph;
import org.gephi.graph.api.GraphController;
import org.gephi.graph.api.GraphModel;
import org.gephi.graph.api.Node;
import org.gephi.io.database.drivers.MySQLDriver;
import org.gephi.io.exporter.api.ExportController;
import org.gephi.io.importer.api.Container;
import org.gephi.io.importer.api.EdgeDefault;
import org.gephi.io.importer.api.ImportController;
import org.gephi.io.importer.plugin.database.EdgeListDatabaseImpl;
import org.gephi.io.importer.plugin.database.ImporterEdgeList;
import org.gephi.io.processor.plugin.DefaultProcessor;
import org.gephi.layout.plugin.AutoLayout;
import org.gephi.layout.plugin.force.StepDisplacement;
import org.gephi.layout.plugin.force.yifanHu.YifanHuLayout;
import org.gephi.partition.api.Partition;
import org.gephi.partition.api.PartitionController;
import org.gephi.partition.plugin.NodeColorTransformer;
import org.gephi.preview.api.PreviewController;
import org.gephi.preview.api.PreviewModel;
import org.gephi.preview.api.PreviewProperty;
import org.gephi.project.api.ProjectController;
import org.gephi.project.api.Workspace;
import org.gephi.statistics.plugin.Modularity;
import org.openide.util.Lookup;

public class TraceMap {
    private static Logger logger = LogManager.getLogger(TraceMap.class.getName());
	
	public static final String DBDRIVER   ="com.mysql.jdbc.Driver";		//定义数据库驱动程序
	public static final String DBURL      ="jdbc:mysql://localhost:3306/wibupt?characterEncoding=UTF-8";	//定义数据库连接地址	
	private static Connection conn        = null;								//声明静态的Connection对象

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
	
	public File datafolder; 	//存储原始文件的总目录
	public File[] monfolders; 	//原始文件总目录下的子目录，即各个监测点的数据文件夹
	public String datafilepath; //存储原始文件的路径的字符串
	public String day;
	public String etime;         //结束时间 current time 
	public String stime;         //起始时间
	public long etimestamp;       //结束时间戳
	public long stimestamp;       //起始时间戳
	public int duration;        //持续时间段，如：15分钟
	public String temppath;
	public String exportpath;
	String groupnames ="";
	
	public void init(String _datapath, String _day,String _etime,int _duration,String _exportpath){
		datafilepath = _datapath;
		day = _day;
		etime = _etime;
		duration = _duration;
		exportpath=_exportpath;
		String nian=day.substring(0,4);
		String yue=day.substring(4,6);
		String ri=day.substring(6,8);
	    String tempTamp=nian+"-"+yue+"-"+ri+" "+etime;     //日期+时间  即结束时间
	    etimestamp=getgelin(tempTamp);           //转换为结束时间戳
		stimestamp=etimestamp-duration*60; 
	}
	
	public void torun(String dfpath, String day) {
		datafilepath = dfpath;

		
		HashMap<String,String> groupindexs = new HashMap<String,String>();
		try{
			String sql1 = "select groupid,groupname from groupindex";
			Statement st = conn.createStatement();
			ResultSet rst = st.executeQuery(sql1);
			while (rst.next()) {
				groupindexs.put(rst.getString(1),rst.getString(2));
				groupnames=groupnames+rst.getString(2)+",";
			}
			groupnames=groupnames.substring(0, groupnames.length()-1);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		logger.info("get groupid,groupname from groupindex first!" );

		// 3,获取每个分组下的监测点；统计监测点数据，将本五分钟内的数据插入到数据库中
		String sql2 = "select monid from monindex where groupid = ?";
		try {
			PreparedStatement pre1 = conn.prepareStatement(sql2);
			Iterator<Entry<String, String>> it = groupindexs.entrySet().iterator();
			while (it.hasNext()) {
				HashSet<String> monids = new HashSet<String>();
				Map.Entry<String,String> a =it.next();
				String id = a.getKey() ;
				String gname = a.getValue() ;
				pre1.setString(1, id);
				ResultSet rs = pre1.executeQuery();
				while (rs.next()) {
					monids.add(rs.getString(1));
				}
				groupcomein(gname, monids, day,stimestamp, etimestamp);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	// 计算每个分组下的comein
	public void groupcomein(String gname, Set<String> monids, String day,
			long begin, long end) {
		LinkedList<String> sourcetime = new LinkedList<String>();
		// 1,读原始文件
		File datafolder = new File(datafilepath);
		if (datafolder == null || !(datafolder.exists())) { // 判断存储原始数据的目录是否存在
			logger.error("Folder: " + datafilepath + "  not found~~ To QUIT!");
			return;
		}
		File[] monfolders = datafolder.listFiles(); // 列出data总目录下的所有监测点的data目录
		if (monfolders == null || monfolders.length == 0) {
			logger.error("No folder found in " + datafolder.getName()
					+ "~~ To QUIT!");
			return;
		}

		for (int i = 0; i < monfolders.length; i++) {
			// 如果此监测点在此分组中，才进行如下的操作
			if (monids.contains(monfolders[i].getName())) {
				double[] rss;
				try {
					rss = getrss(monfolders[i].getName());
				} catch (SQLException e) {
					logger.error("SQLException when retrieve RSS of:"
							+ monfolders[i].getName()
							+ "~~ To IGNORE it's data!");
					continue;
				}
				if (rss == null) {
					logger.error("Null returned when retrieve RSS of:"
							+ monfolders[i].getName()
							+ "~~ To IGNORE it's data!");
					continue;
				}
				double rss_in = rss[0]; // 该monid设定的室内信号强度的小值，大于rss_in的信号都认为是室内手机发出的信号
				double rss_out = rss[1]; // 周边信号强度的小值，大于rss_out的信号都认为是周边手机发出的信号

				logger.info("Begin to analysis: " + monfolders[i].getName()
						+ " with rss_in:" + rss_in + " and rss_out:" + rss_out
						+ " in day:" + day);

				// 打开监测点目录下某天文件~~只处理当天的数据
				File dayfile = new File(monfolders[i].getPath()
						+ System.getProperty("file.separator") + day);
				if (dayfile == null || !(dayfile.exists())) { // 判断文件是否存在
					logger.info("The data file " + dayfile.getName()
							+ " not found!~~ To IGNORE it's data!");
					continue; // 若没有当天数据，接着处理下一个监测点的数据
				}

				// 逐行处理
				FileInputStream fis = null;
				Scanner monscan = null;
				try {
					fis = new FileInputStream(dayfile);
				} catch (FileNotFoundException e) {
					logger.error("FileNotFoundException!! " + dayfile.getName()
							+ " ~~ To IGNORE it's data!");
					continue; // 若没有当天数据，接着处理下一个监测点的数据
				}
				monscan = new Scanner(fis);

				// int count=0;
				while (monscan.hasNextLine()) {
					String line = monscan.nextLine();
					Pattern subpat = Pattern.compile("[|]"); // 创建Pattern实例
					String mac_rss_time[] = subpat.split(line);
					
					if (mac_rss_time.length == 3) {
						try {
							String linemac = mac_rss_time[0];
							long linetime = Long.parseLong(mac_rss_time[2]); // time
							int linerss = Integer.parseInt(mac_rss_time[1]); // rss
							if (linerss > rss_in) {
								if (begin <= linetime && linetime <= end) {
									sourcetime.add(linemac + "|" + linetime);
								}
							}
						} catch (NumberFormatException e) {
							e.getStackTrace();
						}
					}
				}
				monscan.close();
				try {
					if (fis != null)
						fis.close();
				} catch (IOException e) {
					logger.error(e.getMessage());
					continue;
				}
			}
		}
		// 批量导入数据库
		try {
			boolean auto = conn.getAutoCommit();
			conn.setAutoCommit(false);
			Statement stat = conn.createStatement();
			Iterator<String> it = sourcetime.iterator();
			int c=0;
			while (it.hasNext()) {
				String st = it.next();
				c++;
            	Pattern subpat = Pattern.compile("[|]"); // 创建Pattern实例
				String mac_time[] = subpat.split(st);
				int time = Integer.parseInt(mac_time[1]); // time
				String mac = mac_time[0];
				String sql = "insert into edges (source,target,time) values('" + mac + "','" + gname
						+ "','" + time + "')";
				stat.addBatch(sql);
			}
			stat.executeBatch();
			conn.commit();
			logger.info("insert "+gname+" data into DB edges finished");
			conn.setAutoCommit(auto);
		} catch (SQLException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

	}
	
	 public void script(String user,String passwd) {
	    //Init a project - and therefore a workspace
	    ProjectController pc = Lookup.getDefault().lookup(ProjectController.class);
	    pc.newProject();
	    Workspace workspace = pc.getCurrentWorkspace();

	    //Append container to graph structure
	    ImportController importController = Lookup.getDefault().lookup(ImportController.class);
	    AttributeModel attributeModel = Lookup.getDefault().lookup(AttributeController.class).getModel();
	 
	    String[] moniternames=groupnames.split(",");
		
		
		String mosql="select distinct edges.source as source,edges.target as target from edges where '"
				+stimestamp+"' <=time and time<= '"+etimestamp+"'";
		
	    //Import database
	    EdgeListDatabaseImpl db = new EdgeListDatabaseImpl();
	    db.setDBName("wibupt");
	    db.setHost("localhost");
	    db.setUsername(user);
	    db.setPasswd(passwd);
	    db.setSQLDriver(new MySQLDriver());
	    db.setPort(3306); 
	 //   db.setNodeQuery(mosql1);
	    db.setEdgeQuery(mosql);
	    
	    ImporterEdgeList edgeListImporter = new ImporterEdgeList();
	    Container container = importController.importDatabase(db, edgeListImporter);
	    container.setAllowAutoNode(true);                             //Don't create missing nodes
	    container.getLoader().setEdgeDefault(EdgeDefault.DIRECTED);   //Force UNDIRECTED
	       
	    importController.process(container, new DefaultProcessor(), workspace); //Append imported data to GraphAPI     
	    GraphModel graphModel = Lookup.getDefault().lookup(GraphController.class).getModel();//See if graph is well imported
	    DirectedGraph graph = graphModel.getDirectedGraph();
	    
	    
	    int flag=0;
	    	for(Node n : graph.getNodes()){
	    		for(int j=0;j<moniternames.length;j++){
	    			if(n.getNodeData().getLabel().equals(moniternames[j])){
	    				flag=1;
	    			}
	    		}
	    		if(flag==0)
	    			n.getNodeData().setLabel("");
	    		flag=0;
	    	}
	    //Layout for 1 minute 
	    long time = 0;
	    if (graph.getEdgeCount() > 20000) {
			time = (long) (((double) graph.getEdgeCount() / 80));
		}else
		if (graph.getEdgeCount() > 12000) {
			time = (long) (((double) graph.getEdgeCount() / 85));
		} else if (graph.getEdgeCount() > 8000) {
			time = (long) (((double) graph.getEdgeCount() / 90));
		} else if (graph.getEdgeCount() > 5000) {
			time = (long) (((double) graph.getEdgeCount() / 95));
		} else if (graph.getEdgeCount() > 100) {
			time = (long) (((double) graph.getEdgeCount() / 100));
		} else {
			time = 1;
		}
			
		YifanHuLayout layout = new YifanHuLayout(null,new StepDisplacement(1f));
		AutoLayout autoLayout = new AutoLayout(time,TimeUnit.SECONDS);
		autoLayout.setGraphModel(graphModel);
		autoLayout.addLayout(layout, 1f);
		autoLayout.execute();
			
		//Run modularity algorithm - community detection  
		Modularity modularity = new Modularity();
		modularity.execute(graphModel, attributeModel);
	
					 
		//Partition with 'modularity_class', just created by Modularity algorithm  分割
		PartitionController partitionController = Lookup.getDefault().lookup(PartitionController.class);
		AttributeColumn modColumn = attributeModel.getNodeTable().getColumn(Modularity.MODULARITY_CLASS);
		Partition p2 = partitionController.buildPartition(modColumn, graph);
		NodeColorTransformer nodeColorTransformer2 = new NodeColorTransformer();
		nodeColorTransformer2.randomizeColors(p2);
		partitionController.transform(p2, nodeColorTransformer2);	
					
		//Preview configuration  预览
		PreviewController previewController = Lookup.getDefault().lookup(PreviewController.class);
		PreviewModel previewModel = previewController.getModel();
		previewModel.getProperties().putValue(PreviewProperty.SHOW_NODE_LABELS,Boolean.TRUE);
		previewModel.getProperties().putValue(PreviewProperty.EDGE_CURVED, Boolean.FALSE);
		//previewModel.getProperties().putValue(PreviewProperty.EDGE_RESCALE_WEIGHT, Boolean.TRUE);
		previewModel.getProperties().putValue(PreviewProperty.EDGE_THICKNESS,0.5);
		Font ft=new Font("Times New Roman",Font.ITALIC,24);
		previewModel.getProperties().putValue(PreviewProperty.NODE_LABEL_FONT,ft);
		previewController.refreshPreview();
		

	 }
	 public void export(){
	    ExportController ec = Lookup.getDefault().lookup(ExportController.class);//Simple PDF export
	    try {
	       ec.exportFile(new File(exportpath));
	    } catch (IOException ex) {
	        ex.printStackTrace();
	        return;
	    }

	 }
	 
	 public String getYestoday() {
			long time=System.currentTimeMillis();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			String today = sdf.format(new Date(time)); 
			return today; 
		} 
		public double[] getrss(String monid) throws SQLException {
			double[] rss = new double[2];
			PreparedStatement perstat = null;
			ResultSet res = null;
			String sql = "select rssin,rssout FROM monindex where monid='" + monid
					+ "';";
			perstat = conn.prepareStatement(sql); // 实例化Statement对象
			res = perstat.executeQuery();
			while (res.next()) {
				for (int i = 0; i < 2; i++) {
					rss[i] = res.getInt(i + 1);
				}
			}
			return rss;
		}

		public String getTodayStr() {
			long time = System.currentTimeMillis();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			String today = sdf.format(new Date(time));
			return today; // 20140718
		}
		
		
		public  String timechange(String initime) {
			int iniminite = Integer.parseInt(initime.substring(3,5));
			String curminite;
			if (iniminite<30) curminite="00";
			else curminite="30";
			String curtime = initime.substring(0,3)+curminite+initime.substring(5,8);
			return curtime;
		}

		// 将字符串转为时间戳
		// 20140712 --> 类似1405814899的数字

		public long getgelin(String user_time) {
			long re_time = 0;
			String re_time1 = null;
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date d;
			try {
				d = sdf.parse(user_time);
				long l = d.getTime();
				l=l-8*60*60*1000;
				String str = String.valueOf(l);
				re_time1 = str.substring(0, 10);
				re_time = Long.parseLong(re_time1);
			} catch (ParseException e) {
				logger.error(e.getMessage());
			}
			return re_time;
		}

	

	public static void main(String[] args) {
		try{
			TraceMap  ti =new TraceMap();
			String user=args[0];  // db username
			String passwd=args[1];	//db passwd
			String datapath=args[2];//dataPath,eg."D:\\data"
									//args[3]  exportPath eg."D:\\ProgramExport\\"
			String day=args[4];//day,"20140525"
			String time=ti.timechange(args[5]); //????time,"14:26:00"
			
			int duration=Integer.parseInt(args[6]);  //duration,15????
			if (args[4].equals("today")) { // yesterday??????????????20140711???????????????
				day=ti.getYestoday();
			} else {
				day=args[4];
			}
			String exportpath=args[3]+day+time.substring(0, 2)+time.substring(3, 5)+".svg"; 	
			conn = ti.connDatabase(user,passwd);				
			ti.init(datapath,day,time,duration,exportpath);
			ti.torun(datapath,day);
			ti.script(user,passwd);
			ti.export();
		
			Statement st = conn.createStatement();
			String sql = "delete from edges";
			st.execute(sql);
		}catch (Exception e){
			logger.error("error!!!" + e.getMessage());
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
