package dao;

import static db.JdbcUtilLogin.close;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import vo.MemberR;
import vo.MemberSTAT;

public class LoginDAOSTAT {
	
	private static LoginDAOSTAT loginDAOSTAT;
	private Connection con;
	
	private LoginDAOSTAT() {
		// TODO Auto-generated constructor stub
	}
	
	public static LoginDAOSTAT getInstance(){
		if(loginDAOSTAT == null){
			loginDAOSTAT = new LoginDAOSTAT();
		}
		return loginDAOSTAT;
	}
	
	public void setConnection(Connection con){
		this.con = con;
	}
	
	public MemberSTAT selectLoginMemberSTAT(String STATid, String STATpasswd) {
		// TODO Auto-generated method stub
		MemberSTAT loginMemberSTAT = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			pstmt = con.prepareStatement("SELECT * FROM STATusers WHERE STATid = ? AND STATpasswd = password(?)");
			pstmt.setString(1, STATid);
			pstmt.setString(2, STATpasswd);
			rs = pstmt.executeQuery();
			if(rs.next()){
				loginMemberSTAT = new MemberSTAT();
				loginMemberSTAT.setSTATEmail(rs.getString("STATemail"));
				loginMemberSTAT.setSTATId(rs.getString("STATid"));
				loginMemberSTAT.setSTATName(rs.getString("STATname"));
				loginMemberSTAT.setSTATPasswd(rs.getString("STATpasswd"));
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
		return loginMemberSTAT;
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




