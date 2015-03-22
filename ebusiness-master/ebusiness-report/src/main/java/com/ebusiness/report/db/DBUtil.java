package com.ebusiness.report.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBUtil {

	static Connection conn = null;
	static PreparedStatement ps = null;
	static ResultSet rs = null;

	

	// 静态方式取得链接
	public static Connection getConn()  {
	
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		}
		
		String uname = "postgres";
		String passwd = "postgres123";
		String url = "jdbc:postgresql://localhost/ebusiness";
		try {
			conn = DriverManager.getConnection(url, uname, passwd);
			System.out.println("GetConnection success");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return conn;
	}

	public static void release() {

		if (rs != null) {
			try {
				rs.close();
				System.out.println("rs Closed!");
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (ps != null) {
			try {
				ps.close();
				System.out.println("ps Closed!");
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (conn != null) {
			try {
				conn.close();
				System.out.println("conn Closed!");
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

}
