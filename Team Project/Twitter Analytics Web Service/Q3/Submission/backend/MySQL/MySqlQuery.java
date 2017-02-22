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

import org.json.JSONArray;
import org.json.JSONObject;

public class MySqlQuery {
	
	private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private static final String DB_USER = System.getenv("DBUSER");
	private static final String DB_PWD = System.getenv("DBPWD");
	
	private static Map<String, MySqlQuery> shardingMap = new HashMap<String, MySqlQuery>();
	
	private String url;

	private MySqlQuery(String url) {
		try {
			this.url = url;
			Class.forName(JDBC_DRIVER);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public static MySqlQuery getInstance(String url) {
		if (shardingMap.get(url) == null) {
			shardingMap.put(url, new MySqlQuery(url));
		}
		return shardingMap.get(url);
	}
	
	private Connection getConnection() {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection("jdbc:mysql://" + url + "/phase1?useUnicode=true&characterEncoding=UTF-8", DB_USER, DB_PWD);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}
	
	public JSONArray getTweetsByUserId(String id) {
		Connection conn = getConnection();
		Statement stmt = null;
		JSONArray objs = new JSONArray();
		try {
			stmt = conn.createStatement();
			String sql = "SELECT * FROM tweets WHERE userId = '" + id + "'";
			ResultSet rs = stmt.executeQuery(sql);
			rs.next();
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

        byte[] buffer = new byte[in.available()];

        in.read(buffer);
        return new String(buffer, StandardCharsets.UTF_8);
	}

}
