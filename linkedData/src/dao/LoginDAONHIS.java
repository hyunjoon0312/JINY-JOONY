package dao;

import static db.JdbcUtilLogin.close;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import vo.MemberNHIS;

public class LoginDAONHIS {
	
	private static LoginDAONHIS loginDAONHIS;
	private Connection con;
	
	private LoginDAONHIS() {
		// TODO Auto-generated constructor stub
	}
	
	public static LoginDAONHIS getInstance(){
		if(loginDAONHIS == null){
			loginDAONHIS = new LoginDAONHIS();
		}
		return loginDAONHIS;
	}
	
	public void setConnection(Connection con){
		this.con = con;
	}
	
	public MemberNHIS selectLoginMemberNHIS(String NHISid, String NHISpasswd) {
		// TODO Auto-generated method stub
		MemberNHIS loginMemberNHIS = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			pstmt = con.prepareStatement("SELECT * FROM NHISusers WHERE NHISid = ? AND NHISpasswd = password(?)");
			pstmt.setString(1, NHISid);
			pstmt.setString(2, NHISpasswd);
			rs = pstmt.executeQuery();
			if(rs.next()){
				loginMemberNHIS = new MemberNHIS();
				loginMemberNHIS.setNHISEmail(rs.getString("NHISemail"));
				loginMemberNHIS.setNHISId(rs.getString("NHISid"));
				loginMemberNHIS.setNHISName(rs.getString("NHISname"));
				loginMemberNHIS.setNHISPasswd(rs.getString("NHISpasswd"));
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		finally{
			try {
				close(rs);
				close(pstmt);
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}
		}
		return loginMemberNHIS;
	}
	
	public MemberNHIS getMemberNHIS(String NHISid) {
		// TODO Auto-generated method stub
		MemberNHIS loginMemberNHIS = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			pstmt = con.prepareStatement("SELECT * FROM NHISusers WHERE NHISid = ?");
			pstmt.setString(1, NHISid);
			rs = pstmt.executeQuery();
			if(rs.next()){
				loginMemberNHIS = new MemberNHIS();
				loginMemberNHIS.setNHISEmail(rs.getString("NHISemail"));
				loginMemberNHIS.setNHISId(rs.getString("NHISid"));
				loginMemberNHIS.setNHISName(rs.getString("NHISname"));
				loginMemberNHIS.setNHISPasswd(rs.getString("NHISpasswd"));
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		finally{
			try {
				close(rs);
				close(pstmt);
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}
		}
		return loginMemberNHIS;
	}
	
}




