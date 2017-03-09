package svc;

import static db.JdbcUtilLogin.*;
import java.sql.Connection;
import dao.LoginDAOLINK;
import vo.MemberLINK;
public class LoginServiceLINK {

	public MemberLINK getLoginMember(String LINKid, String LINKpasswd) {
		// TODO Auto-generated method stub
		LoginDAOLINK loginDAOLINK = LoginDAOLINK.getInstance();
		Connection con = getLoginConnection();
		loginDAOLINK.setConnection(con);
		MemberLINK loginMemberLINK = loginDAOLINK.selectLoginMemberLINK(LINKid,LINKpasswd);
		close(con);
		return loginMemberLINK;
	}
	
}








