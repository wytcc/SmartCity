package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SQLModule {
	public String drivename="com.mysql.jdbc.Driver";
	public String url="jdbc:mysql://10.76.0.182:3306/";
    //public String url="jdbc:mysql://10.76.0.193:3306/";1234567890
    public String user="root";
    public String password="123456789";
    public Connection conn;

	
	public SQLModule() {
	}
	
	public Connection connect() {
		try {
			url += "?useServerPrepStmts=false&rewriteBatchedStatements=true";
			Class.forName(drivename);
			conn = (Connection) DriverManager
					.getConnection(url, user, password);
			if (!conn.isClosed()) {
				System.out.println("Succeeded connecting to the Database!");
			} else {
				System.out.println("Falled connecting to the Database!");
			}
			conn.setAutoCommit(false); 
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return conn;
	}
	
	public void disconnect() {
		try {
			
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

}
