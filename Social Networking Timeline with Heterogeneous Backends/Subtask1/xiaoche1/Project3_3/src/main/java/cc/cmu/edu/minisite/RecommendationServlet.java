package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;



public class RecommendationServlet extends HttpServlet {

    private static String zkAddr = "172.31.1.172";
    private static TableName tableName = TableName.valueOf("task4");
    private static Table linksTable;
    private static Connection conn;
    private static final Logger LOGGER = Logger.getRootLogger();
    private static byte[] bColFamily = Bytes.toBytes("links");
	
	public RecommendationServlet () throws Exception {
        /*
        	Your initialization code goes here
         */
		
	}

	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) 
			throws ServletException, IOException {
		JSONObject result = new JSONObject();
		String id = request.getParameter("id");
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
	    JSONArray recomJSONArray = new JSONArray();
		byte[] bCol = Bytes.toBytes("followee");
		byte[] bColFollower = Bytes.toBytes("follower");
		byte[] bColProfile = Bytes.toBytes("profile");
		byte[] bColID = Bytes.toBytes("id");
		byte[] bColName = Bytes.toBytes("user_name");
		List<String> followees = new ArrayList<String>();
		List<String> followers = new ArrayList<String>();
		Get getFollowee = new Get(id.getBytes());
		Result rs = linksTable.get(getFollowee);
		//get follower column
		String followerString = Bytes.toString(rs.getValue(bColFamily, bColFollower));
		for(String s: followerString.split("\\|")){
			if(s.length() > 0){
				followers.add(s);
			}
		}
		//get followee column
		String followeeString = Bytes.toString(rs.getValue(bColFamily, bCol));
		
        for(String s: followeeString.split("\\|")){
        		if(s.length() > 0){ // exclude those who are followers
        			followees.add(s);
        		}
        }
        
        Map<String, Integer> recom = new HashMap<String, Integer>();
        for(int i = 0; i < followees.size(); i++){
        		String s = followees.get(i);
        		Get getRecom = new Get(s.getBytes());
        		Result resultRecom = linksTable.get(getRecom);
        		String recomString = Bytes.toString(resultRecom.getValue(bColFamily, bCol)); 
        		for(String r: recomString.split("\\|")){
        			// length larger than 1
        			if(r.length() > 0 && !r.equals(id) && !followees.contains(r)){
        				recom.putIfAbsent(r, 0);
        				recom.put(r, recom.get(r)+1);
        			}
        		}
        }
        

        
        //sort the map by value and get recommandation
        //cited[4]
        List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(recom.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>(){
        		@Override
        		public int compare(Map.Entry<String, Integer> a, Map.Entry<String, Integer> b){
        			if(a.getValue().equals(b.getValue())){
        				return Integer.valueOf(a.getKey()).compareTo(Integer.valueOf(b.getKey()));
        			}else{
        				return b.getValue().compareTo(a.getValue());
        			}
        		}
        	});
        
        //put sorted result into an LinkedList
        List<String> recomList = new LinkedList<String>();
        int count = 0;
        for(Map.Entry<String, Integer>entry:list){
        		recomList.add(entry.getKey());
        		count++;
        		if(count == 10){
        			break;
        		}
        }
        
        //Obtain profile for each id
        for(int i = 0; i < recomList.size(); i++){
        		JSONObject recomObject = new JSONObject();
        		Get ID = new Get(recomList.get(i).getBytes());
        		Result recomProfile = linksTable.get(ID);
        		String profile = Bytes.toString(recomProfile.getValue(bColFamily, bColProfile));
        		String name = Bytes.toString(recomProfile.getValue(bColFamily, bColName));
        		recomObject.put("name",name);
        		recomObject.put("profile", profile);
        		recomJSONArray.put(recomObject);
        }
        result.put("recommendation", recomJSONArray);
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

