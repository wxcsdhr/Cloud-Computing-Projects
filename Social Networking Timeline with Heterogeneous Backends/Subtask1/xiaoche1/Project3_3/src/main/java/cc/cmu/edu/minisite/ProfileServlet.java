package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.json.JSONObject;

public class ProfileServlet extends HttpServlet {
	private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private static final String DB_NAME = "task1";
	private static final String URL = "jdbc:mysql://task1.cqa8mbro1fkr.us-east-1.rds.amazonaws.com/";
	private static final String USER_NAME = System.getenv("USER_NAME");
	private static final String USER_PASSWORD = System.getenv("USER_PASSWORD");
	private static Connection Mysqlconn = null;

	
    public ProfileServlet() {

    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response) 
            throws ServletException, IOException {
        JSONObject result = new JSONObject();
    	try {
			Class.forName(JDBC_DRIVER);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	try {
			Mysqlconn = DriverManager.getConnection(URL + DB_NAME + "?useSSL=false", USER_NAME, USER_PASSWORD);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        String id = request.getParameter("id");
        String pwd = request.getParameter("pwd");
        Statement stmt = null;
        try{
        	stmt = Mysqlconn.createStatement();
            String table1 = "users";
            String table2 = "usersinfo";
            String sql = "SELECT user_profile, user_name FROM " + table2 + " WHERE user_id = (SELECT user_id FROM " + table1 + " WHERE user_id = " +id + " AND password = '" + pwd + "')";
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
			if(profile == null){ // no such data
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
