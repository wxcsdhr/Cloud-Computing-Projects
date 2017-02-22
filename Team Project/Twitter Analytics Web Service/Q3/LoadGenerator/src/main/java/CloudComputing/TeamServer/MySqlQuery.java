package CloudComputing.TeamServer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Blob;
import java.nio.charset.StandardCharsets;
import java.io.InputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

import io.vertx.core.MultiMap;

public class MySqlQuery {
	
	private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private static final String DB_USER = System.getenv("DBUSER");
	private static final String DB_PWD = System.getenv("DBPWD");
	
	private static MySqlQuery INSTANCE = new MySqlQuery();
	
	private MySqlQuery() {
		try {
			Class.forName(JDBC_DRIVER);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public static MySqlQuery getInstance() {
		return INSTANCE;
	}
	
	private Connection getConnection() {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection("jdbc:mysql://localhost/phase3?useSSL=false", DB_USER, DB_PWD);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}
	
	public String queryRange(MultiMap map) throws SQLException {
		String pre1 = map.get("p1").toLowerCase();
		String pre2 = map.get("p2").toLowerCase();
		String pre3 = map.get("p3").toLowerCase();
		String ds = map.get("date_start").replaceAll("-", "");
		String de = map.get("date_end").replaceAll("-", "");
		String ts = map.get("tid_start");
		String te = map.get("tid_end");
		String us = map.get("uid_start");
		String ue = map.get("uid_end");
		return getRangeResult(pre1, pre2, pre3, ds, de, ts, te, us, ue);
	}
	
	private String getRangeResult(String pre1, String pre2, String pre3, String ds, String de, String ts, String te, String us, String ue) throws SQLException {
		Connection conn = getConnection();
		Statement stmt = conn.createStatement();
		String sql = "SELECT * FROM q3tweets WHERE tweetId BETWEEN " + ts + " AND " + te + " AND userId BETWEEN " + us + " AND " + ue  + " AND createdAt BETWEEN " + ds + " AND " + de + " AND wc REGEXP '\"(" + pre1 + "|" + pre2 + "|" + pre3 + ")'";
		ResultSet rs = stmt.executeQuery(sql);
		StringBuilder result = new StringBuilder();
        Map<String, Integer> countMap = new HashMap<>();
        countMap.put(pre1, 0);
        countMap.put(pre2, 0);
        countMap.put(pre3, 0);
		while (rs.next()) {
			JSONObject obj = new JSONObject(rs.getString("wc"));
			Set<String>keys = obj.keySet();
			for (String key : keys) {
				if (key.startsWith(pre1)){
                    countMap.put(pre1, countMap.get(pre1) + obj.getInt(key));
                }
                else if (key.startsWith(pre2)){
                    countMap.put(pre2, countMap.get(pre2) + obj.getInt(key));
                }
                else if (key.startsWith(pre3)){
                    countMap.put(pre3, countMap.get(pre3) + obj.getInt(key));
                }
			}
		}
		rs.close();
		stmt.close();
		return result.append(pre1 + ":" + countMap.get(pre1) + "\n")
	         	  .append(pre2 + ":" + countMap.get(pre2) + "\n")
	         	  .append(pre3 + ":" + countMap.get(pre3) + "\n").toString();
	}
	
	public JSONArray getTweetsByUserId(String id) {
		Connection conn = getConnection();
		Statement stmt = null;
		JSONArray objs = new JSONArray();
		try {
			stmt = conn.createStatement();
			String sql = "SELECT * FROM tweets WHERE userId = '" + id + "'";
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				String content = readContent(rs);
				String wordCount = rs.getString("wordCount");
				
				String[] contents = content.split("!");
				for (String c : contents) {
					String[] sc = c.split(",");
					JSONObject obj = new JSONObject();
					obj.put("tweetId", sc[0]);
		            obj.put("createdAt", sc[1]);
		            obj.put("censoredText", sc[2]);
		            obj.put("impactScore", sc[4]);
		            obj.put("wordCount", wordCount);
		            objs.put(obj);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return objs;
	}

	private String readContent(ResultSet rs) throws SQLException, IOException{
		Blob blob = rs.getBlob("content");
        InputStream in = blob.getBinaryStream();

        byte[] buffer = new byte[3072];

        in.read(buffer);
        return new String(buffer, StandardCharsets.UTF_8);
	}

}
