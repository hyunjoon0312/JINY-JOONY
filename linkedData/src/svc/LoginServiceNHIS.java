package svc;

import static db.JdbcUtilLogin.close;
import static db.JdbcUtilLogin.getLoginConnection;

import java.sql.Connection;

import dao.LoginDAONHIS;
import vo.MemberNHIS;
public class LoginServiceNHIS {

	public MemberNHIS getLoginMember(String NHISid, String NHISpasswd) {
		// TODO Auto-generated method stub
		LoginDAONHIS loginDAONHIS = LoginDAONHIS.getInstance();
		Connection con = getLoginConnection();
		loginDAONHIS.setConnection(con);
		MemberNHIS loginMemberNHIS = loginDAONHIS.selectLoginMemberNHIS(NHISid,NHISpasswd);
		close(con);
		return loginMemberNHIS;
	}
	
}








