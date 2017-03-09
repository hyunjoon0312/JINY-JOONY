package dao;

import static db.JdbcUtilLogin.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import vo.MemberNEOK;

public class LoginDAONEOK {
	
	private static LoginDAONEOK loginDAONEOK;
	private Connection NEOKcon;
	
	private LoginDAONEOK() {
		// TODO Auto-generated constructor stub
	}
	
	public static LoginDAONEOK getInstance(){
		if(loginDAONEOK == null){
			loginDAONEOK = new LoginDAONEOK();
		}
		return loginDAONEOK;
	}
	
	public void setConnection(Connection NEOKcon){
		this.NEOKcon = NEOKcon;
	}
	
	public MemberNEOK selectLoginMemberNEOK(String NEOKid, String NEOKpasswd) {
		// TODO Auto-generated method stub
		MemberNEOK loginMemberNEOK = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			pstmt = NEOKcon.prepareStatement("SELECT * FROM NEOKusers WHERE NEOKid = ? AND NEOKpasswd = password(?)");
			pstmt.setString(1, NEOKid);
			pstmt.setString(2, NEOKpasswd);
			rs = pstmt.executeQuery();
			if(rs.next()){
				loginMemberNEOK = new MemberNEOK();
				loginMemberNEOK.setNEOKEmail(rs.getString("NEOKemail"));
				loginMemberNEOK.setNEOKId(rs.getString("NEOKid"));
				loginMemberNEOK.setNEOKName(rs.getString("NEOKname"));
				loginMemberNEOK.setNEOKPasswd(rs.getString("NEOKpasswd"));
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
		return loginMemberNEOK;
	}
	
}




