package CloudComputing.TeamServer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;


public class MySQLQuery4 {

	private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private static final String DB_USER = System.getenv("DBUSER");
	private static final String DB_PWD = System.getenv("DBPWD");
	private static Connection conn = null;
	
	public MySQLQuery4() {
		try {
			Class.forName(JDBC_DRIVER);
			getConnection();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private void getConnection() {
		try {
			conn = DriverManager.getConnection("jdbc:mysql://localhost/phase3?useSSL=false", DB_USER, DB_PWD);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public String read(String tweetid, String field) throws SQLException{
		ResultSet rs = conn.createStatement().executeQuery("SELECT " + field + " FROM q4tweets WHERE tweetId = " + tweetid);
		rs.next();
		if (rs.wasNull()){
		    rs.close();
		    return "";
		}

        return rs.getString(field);
	}
	
	public void delete(String tweetid, String field) throws SQLException{
		write(tweetid, field, "NULL");
	}
	
	public void write(String tweetid, String field, String payload) throws SQLException{
		conn.createStatement().execute("UPDATE q4tweets SET " + field + "='" + payload + "' WHERE tweetid=" + tweetid);
	}
}
