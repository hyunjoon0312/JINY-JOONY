package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

//데이터베이스 작업을 할 때 반복적으로 수행해야 하는 작업을 정의하는 클래스
public class JdbcUtilSTAT {
	
	public static Connection getSTATConnection(){
		
		Connection con =null;
		
		try{
			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection("jdbc:mysql://112.72.158.187:3306/stat_take_data", "hyunjoon",
					"hyunjoon");
			System.out.println("stat_link connect success");
		}catch(Exception e){
			e.printStackTrace();
		}
		return con;

	}

	
	public static void close(Connection con){
		try {
			con.close();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	public static void close(PreparedStatement pstmt){
		try {
			pstmt.close();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	public static void close(ResultSet rs){
		try {
			rs.close();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	public static void commit(Connection con){
		try {
			con.commit();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	public static void rollback(Connection con){
		try {
			con.rollback();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
}











