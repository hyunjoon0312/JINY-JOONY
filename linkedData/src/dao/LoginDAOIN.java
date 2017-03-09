package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import vo.MemberIN;
import static db.JdbcUtilLogin.*;

public class LoginDAOIN {
	
	private static LoginDAOIN loginDAOIN;
	private Connection con;
	
	private LoginDAOIN() {
		// TODO Auto-generated constructor stub
	}
	
	public static LoginDAOIN getInstance(){
		if(loginDAOIN == null){
			loginDAOIN = new LoginDAOIN();
		}
		return loginDAOIN;
	}
	
	public void setConnection(Connection con){
		this.con = con;
	}
	
	public MemberIN selectLoginMemberIN(String INid, String INpasswd) {
		// TODO Auto-generated method stub
		MemberIN loginMemberIN = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			pstmt = con.prepareStatement("SELECT * FROM INusers WHERE INid = ? AND INpasswd = password(?)");
			pstmt.setString(1, INid);
			pstmt.setString(2, INpasswd);
			rs = pstmt.executeQuery();
			if(rs.next()){
				loginMemberIN = new MemberIN();
				loginMemberIN.setINEmail(rs.getString("INemail"));
				loginMemberIN.setINId(rs.getString("INid"));
				loginMemberIN.setINName(rs.getString("INname"));
				loginMemberIN.setINPasswd(rs.getString("INpasswd"));
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
		return loginMemberIN;
	}
	
}




