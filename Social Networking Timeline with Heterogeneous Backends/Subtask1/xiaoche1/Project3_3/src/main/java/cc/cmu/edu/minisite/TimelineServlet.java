package cc.cmu.edu.minisite;

import static com.mongodb.client.model.Sorts.ascending;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Sorts.descending;;


public class TimelineServlet extends HttpServlet {
	
	//mySQL connection
	private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private static final String DB_NAME = "task1";
	private static final String URL = "jdbc:mysql://task1.cqa8mbro1fkr.us-east-1.rds.amazonaws.com/";
	private static final String USER_NAME = System.getenv("USER_NAME");
	private static final String USER_PASSWORD = System.getenv("USER_PASSWORD");
	
	//HBase connection
	private static Connection mySQLconn = null;
    private static String zkAddr = "172.31.1.172";
    private static TableName tableName = TableName.valueOf("task4");
    private static Table linksTable;
    private static org.apache.hadoop.hbase.client.Connection conn;
    private static final Logger LOGGER = Logger.getRootLogger();
    private static byte[] bColFamily = Bytes.toBytes("links");
    
    //MongoDB connection
    MongoClient mongoClient = new MongoClient(new ServerAddress("ec2-52-90-24-137.compute-1.amazonaws.com", 27017));
    MongoDatabase db = mongoClient.getDatabase("task3");
    
    public TimelineServlet() throws Exception {
        /*
            Your initialization code goes here
        */
    }

    @Override
    protected void doGet(final HttpServletRequest request,
            final HttpServletResponse response) throws ServletException, IOException {

        JSONObject result = new JSONObject();
        String id = request.getParameter("id");
        
        
        
        // connect mysql
    	try {
			Class.forName(JDBC_DRIVER);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	try {
			mySQLconn = DriverManager.getConnection(URL + DB_NAME + "?useSSL=false", USER_NAME, USER_PASSWORD);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        Statement stmt = null;
        try{
        	stmt = mySQLconn.createStatement();
            String table1 = "users";
            String table2 = "usersinfo";
            String sql = "SELECT user_profile, user_name FROM " + table2 + " WHERE user_id = (SELECT user_id FROM " + table1 + " WHERE user_id = " +id + ")";
            String profile = null;
            String name = null;
            ResultSet rs = stmt.executeQuery(sql);
			if(!rs.isBeforeFirst()){
				profile = null;
			}else if(rs.next()){
				profile = rs.getString("user_profile");
				name = rs.getString("user_name");
				
	        }
			stmt.close();
			if(profile == null){
	        	id = "Unauthorized";
	        	profile = "#";
	        	result.put("profile", profile);
	        	result.put("name", name);
	            
	        }else{
	        	result.put("profile", profile);
	        	result.put("name", name);
	        }
        }catch(SQLException e){
        	e.printStackTrace();
        }
        
        //Obtain all followers
    	LOGGER.setLevel(Level.ERROR);
    	if(!zkAddr.matches("\\d+.\\d+.\\d+.\\d+")){
    		System.out.println("Malformed HBase IP address");
    		System.exit(-1);
    	}
    	Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", zkAddr + ":16000");
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
        conn = ConnectionFactory.createConnection(conf);
        linksTable = conn.getTable(tableName);
        JSONObject tempResult = new JSONObject();
        JSONArray followersJSONArray = new JSONArray();
		byte[] bCol = Bytes.toBytes("follower");
		byte[] bColProfile = Bytes.toBytes("profile");
		byte[] bColName = Bytes.toBytes("user_name");
		byte[] bColFollowee = Bytes.toBytes("followee");
		List<String> followers = new ArrayList<String>(); // record every follower
		List<String> followees = new ArrayList<String>(); // record every followee
		Get getFollower = new Get(id.getBytes());
		Result rs = linksTable.get(getFollower);
		String followerString = Bytes.toString(rs.getValue(bColFamily, bCol));
		String followeeString = Bytes.toString(rs.getValue(bColFamily, bColFollowee));

        for(String s: followerString.split("\\|")){
        		if(s.length() > 0){
        			followers.add(s);
        		}
        }
        
        for(String s: followeeString.split("\\|")){
        	if(s.length() > 0){
        		followees.add(s);
        	}
        }
        // Query profile of each folloewr
        for(String s: followers){
    		Get getProfile = new Get(s.getBytes());
    		Result rsProfile = linksTable.get(getProfile);
    		String profile = Bytes.toString(rsProfile.getValue(bColFamily, bColProfile));
    		String name = Bytes.toString(rsProfile.getValue(bColFamily, bColName));
    		tempResult.put("name", name);
    		tempResult.put("profile", profile);
    		followersJSONArray.put(tempResult);
    		tempResult = new JSONObject();
        }
        
        // Convert to List
        List<JSONObject> followerJSONList = new ArrayList<JSONObject>();
        for(int i = 0; i < followersJSONArray.length(); i++){
        	followerJSONList.add(followersJSONArray.getJSONObject(i));
        }
        
        //Start sorting 
        Collections.sort(followerJSONList, new Comparator<JSONObject>(){
        		private String name = "name";
        		private String profile = "profile";
        		
        		@Override
        		public int compare(JSONObject a, JSONObject b){
        			String nameA = a.getString(name);
        			String nameB = b.getString(name);
        			String profileA = a.getString(profile);
        			String profileB = b.getString(profile);
        			if(nameA.equals(nameB)){
        				return profileA.compareTo(profileB);
        			}else{
        				return nameA.compareTo(nameB);
        			}
        		}
        });
        //output result
        JSONArray sortedJSONArray= new JSONArray();
        for(int i = 0; i < followersJSONArray.length();i++){
        		sortedJSONArray.put(followerJSONList.get(i));
        }
        result.put("followers", sortedJSONArray);
        
        //connect to mongodb
        String collections = "posts";
        List<JSONObject> postsList = new ArrayList<JSONObject>();
        JSONArray postArray = new JSONArray();
        for(int i = 0; i < followees.size(); i++){
        		String followeeId = followees.get(i);
        		FindIterable<Document> it = db.getCollection(collections).find(new Document("uid", Integer.parseInt(followeeId))).sort(descending("timestamp")).limit(30);
        		for(Document d: it){
        			postsList.add(new JSONObject(d.toJson()));
        		}
        }

        //sort post 
        Collections.sort(postsList, new Comparator<JSONObject>(){
    		private String timestamp = "timestamp";
    		private String postID = "pid";
    		
    		@Override
    		public int compare(JSONObject a, JSONObject b){
    			String timestampA = a.getString(timestamp);
    			String timestampB = b.getString(timestamp);
    			Integer postIDA = Integer.valueOf(a.getInt(postID));
    			Integer postIDB = Integer.valueOf(b.getInt(postID));
    			if(timestampA.equals(timestampB)){
    				return postIDA.compareTo(postIDB);
    			}else{
    				return timestampA.compareTo(timestampB);
    			}
    		}
        });
        int count = 0; //count 30
        for(int i = Math.max(postsList.size()-30, 0); i < postsList.size(); i ++){
        	if(count == 30){
        		break;
        	}
        	postArray.put(postsList.get(i));
        	count++;
        }
        
        result.put("posts", postArray);
        PrintWriter out = response.getWriter();
        out.print(String.format("returnRes(%s)", result.toString()));
        out.close();
    }

    @Override
    protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
    }
    
}

