package svc;

import static db.JdbcUtilLogin.close;
import static db.JdbcUtilLogin.getLoginConnection;

import java.sql.Connection;

import dao.LoginDAOSTAT;
import vo.MemberSTAT;
public class LoginServiceSTAT {

	public MemberSTAT getLoginMember(String STATid, String STATpasswd) {
		// TODO Auto-generated method stub
		LoginDAOSTAT loginDAOSTAT = LoginDAOSTAT.getInstance();
		Connection con = getLoginConnection();
		loginDAOSTAT.setConnection(con);
		MemberSTAT loginMemberSTAT = loginDAOSTAT.selectLoginMemberSTAT(STATid,STATpasswd);
		close(con);
		return loginMemberSTAT;
	}
	
}








