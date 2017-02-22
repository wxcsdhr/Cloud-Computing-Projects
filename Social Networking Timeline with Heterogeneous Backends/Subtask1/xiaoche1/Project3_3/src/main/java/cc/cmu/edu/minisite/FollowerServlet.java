package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

public class FollowerServlet extends HttpServlet {

    private static String zkAddr = "172.31.1.172";
    private static TableName tableName = TableName.valueOf("task2");
    private static Table linksTable;
    private static Connection conn;
    private static final Logger LOGGER = Logger.getRootLogger();
    private static byte[] bColFamily = Bytes.toBytes("links");
    
    public FollowerServlet() throws IOException {
    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {

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
		        String id = request.getParameter("id");
		        JSONObject result = new JSONObject();
		        JSONObject tempResult = new JSONObject();
		        JSONArray followersJSONArray = new JSONArray();

        /*
            Task 2:
            Implement your logic to retrive the followers of this user. 
            You need to send back the Name and Profile Image URL of his/her Followers.

            You should sort the followers alphabetically in ascending order by Name. 
            If there is a tie in the followers name, 
	    sort alphabetically by their Profile Image URL in ascending order. 
        */
		byte[] bCol = Bytes.toBytes("follower");
		byte[] bColProfile = Bytes.toBytes("profile");
		byte[] bColName = Bytes.toBytes("user_name");
		List<String> followers = new ArrayList<String>(); // record every follower
		Get getFollower = new Get(id.getBytes());
		Result rs = linksTable.get(getFollower);
		String followerString = Bytes.toString(rs.getValue(bColFamily, bCol));
        for(String s: followerString.split("\\|")){
        		if(s.length() > 0){
        			followers.add(s);
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
        PrintWriter writer = response.getWriter();
        writer.write(String.format("returnRes(%s)", result.toString()));
        writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        doGet(request, response);
    }   
    
}


