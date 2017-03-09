package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import vo.MemberLINK;
import static db.JdbcUtilLogin.*;

public class LoginDAOLINK {
	
	private static LoginDAOLINK loginDAOLINK;
	private Connection con;
	
	private LoginDAOLINK() {
		// TODO Auto-generated constructor stub
	}
	
	public static LoginDAOLINK getInstance(){
		if(loginDAOLINK == null){
			loginDAOLINK = new LoginDAOLINK();
		}
		return loginDAOLINK;
	}
	
	public void setConnection(Connection con){
		this.con = con;
	}
	
	public MemberLINK selectLoginMemberLINK(String LINKid, String LINKpasswd) {
		// TODO Auto-generated method stub
		MemberLINK loginMemberLINK = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			pstmt = con.prepareStatement("SELECT * FROM LINKusers WHERE LINKid = ? AND LINKpasswd = password(?)");
			pstmt.setString(1, LINKid);
			pstmt.setString(2, LINKpasswd);
			rs = pstmt.executeQuery();
			if(rs.next()){
				loginMemberLINK = new MemberLINK();
				loginMemberLINK.setLINKEmail(rs.getString("LINKemail"));
				loginMemberLINK.setLINKId(rs.getString("LINKid"));
				loginMemberLINK.setLINKName(rs.getString("LINKname"));
				loginMemberLINK.setLINKPasswd(rs.getString("LINKpasswd"));
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
		return loginMemberLINK;
	}
	
}




