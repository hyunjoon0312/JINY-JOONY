package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import vo.MemberR;
import static db.JdbcUtilLogin.*;

public class LoginDAOR {
	
	private static LoginDAOR loginDAOR;
	private Connection con;
	
	private LoginDAOR() {
		// TODO Auto-generated constructor stub
	}
	
	public static LoginDAOR getInstance(){
		if(loginDAOR == null){
			loginDAOR = new LoginDAOR();
		}
		return loginDAOR;
	}
	
	public void setConnection(Connection con){
		this.con = con;
	}
	
	public MemberR selectLoginMemberR(String Rid, String Rpasswd) {
		// TODO Auto-generated method stub
		MemberR loginMemberR = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			pstmt = con.prepareStatement("SELECT * FROM Rusers WHERE Rid = ? AND Rpasswd = password(?)");
			pstmt.setString(1, Rid);
			pstmt.setString(2, Rpasswd);
			rs = pstmt.executeQuery();
			if(rs.next()){
				loginMemberR = new MemberR();
				loginMemberR.setREmail(rs.getString("Remail"));
				loginMemberR.setRId(rs.getString("Rid"));
				loginMemberR.setRName(rs.getString("Rname"));
				loginMemberR.setRPasswd(rs.getString("Rpasswd"));
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
		return loginMemberR;
	}
	
	public MemberR getMemberR(String Rid) {
		// TODO Auto-generated method stub
		MemberR loginMemberR = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			pstmt = con.prepareStatement("SELECT * FROM Rusers WHERE Rid = ?");
			pstmt.setString(1, Rid);
			rs = pstmt.executeQuery();
			if(rs.next()){
				loginMemberR = new MemberR();
				loginMemberR.setREmail(rs.getString("Remail"));
				loginMemberR.setRId(rs.getString("Rid"));
				loginMemberR.setRName(rs.getString("Rname"));
				loginMemberR.setRPasswd(rs.getString("Rpasswd"));
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
		return loginMemberR;
	}
	
}




