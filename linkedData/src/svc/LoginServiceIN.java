package svc;

import static db.JdbcUtilLogin.close;
import static db.JdbcUtilLogin.getLoginConnection;

import java.sql.Connection;

import dao.LoginDAOIN;
import vo.MemberIN;
public class LoginServiceIN {

	public MemberIN getLoginMember(String INid, String INpasswd) {
		// TODO Auto-generated method stub
		LoginDAOIN loginDAOIN = LoginDAOIN.getInstance();
		Connection con = getLoginConnection();
		loginDAOIN.setConnection(con);
		MemberIN loginMemberIN = loginDAOIN.selectLoginMemberIN(INid,INpasswd);
		close(con);
		return loginMemberIN;
	}
	
}








