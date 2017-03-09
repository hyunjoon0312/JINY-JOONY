package svc;

import static db.JdbcUtilLogin.*;
import java.sql.Connection;
import dao.LoginDAOR;
import vo.MemberR;
public class LoginServiceR {

	public MemberR getLoginMember(String Rid, String Rpasswd) {
		// TODO Auto-generated method stub
		LoginDAOR loginDAOR = LoginDAOR.getInstance();
		Connection con = getLoginConnection();
		loginDAOR.setConnection(con);
		MemberR loginMemberR = loginDAOR.selectLoginMemberR(Rid,Rpasswd);
		close(con);
		return loginMemberR;
	}
	
}








