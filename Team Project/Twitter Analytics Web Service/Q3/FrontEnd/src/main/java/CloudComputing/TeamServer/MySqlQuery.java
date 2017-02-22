package CloudComputing.TeamServer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.json.JSONObject;

import io.vertx.core.MultiMap;

public class MySqlQuery {
	
	private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private static final String DB_USER = System.getenv("DBUSER");
	private static final String DB_PWD = System.getenv("DBPWD");
	
	private static final String SQL = "SELECT wc FROM q3tweets WHERE tweetId BETWEEN ? AND ? AND userId BETWEEN ? AND ? AND createdAt BETWEEN ? AND ?";
	
	private static MySqlQuery INSTANCE = new MySqlQuery();
	private Connection conn;
	
	private MySqlQuery() {
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection("jdbc:mysql://localhost/phase3?useSSL=false", DB_USER, DB_PWD);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static MySqlQuery getInstance() {
		return INSTANCE;
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
		PreparedStatement stmt = conn.prepareStatement(SQL);
		stmt.setFetchSize(50);
		stmt.setLong(1, Long.parseLong(ts));
		stmt.setLong(2, Long.parseLong(te));
		stmt.setLong(3, Long.parseLong(us));
		stmt.setLong(4, Long.parseLong(ue));
		stmt.setLong(5, Long.parseLong(ds));
		stmt.setLong(6, Long.parseLong(de));
		ResultSet rs = stmt.executeQuery();
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

}
