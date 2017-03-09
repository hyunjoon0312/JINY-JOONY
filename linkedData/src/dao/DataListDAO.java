package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;

import org.apache.jasper.tagplugins.jstl.core.Catch;

import db.JdbcUtilUpload;
import jdk.nashorn.internal.scripts.JD;
import vo.MemberList;

public class DataListDAO {

	private Connection con;
	private PreparedStatement pstmt;
	private ResultSet rs;

	public DataListDAO() {
	}
	
	//데이터 DB에서 리스트 형태로 불러와서 저장.

	public ArrayList<MemberList> getListAll(){
	
	ArrayList<MemberList> list = new ArrayList<MemberList>();
	
	try{
		con = JdbcUtilUpload.getUploadConnection();
		String sql = "Select * from uploadFile.UploadFileInfo";
		pstmt = con.prepareStatement(sql);
		rs = pstmt.executeQuery();
		while(rs.next()){
			Timestamp time = rs.getTimestamp(1);
			String subject = rs.getString(2);
			String filename = rs.getString(3);
			String uploaderid = rs.getString(6);
			String uploadername = rs.getString(7);
			int nhis = rs.getInt(8);
			int stat = rs.getInt(9);
			MemberList member = new MemberList(time, subject, filename, uploaderid, uploadername, nhis, stat);
			list.add(member);
		}
		System.err.println(list.size());
	}catch (SQLException e) {
		// TODO: handle exception
		e.printStackTrace();
	}finally {
		if(rs!=null){JdbcUtilUpload.close(rs);}
		if(pstmt != null){JdbcUtilUpload.close(pstmt);}
		if(con != null){JdbcUtilUpload.close(con);}
	}
	return list;
	}

		// 리스트 형태로 불러온 데이터를 이렇게 출력가능.
	public static void main(String[] args) {
		DataListDAO dao = new DataListDAO();
		ArrayList<MemberList> list = dao.getListAll();
		System.out.println(list.get(0).getFilename());
		System.out.println(list.get(1).getFilename());
		System.out.println(list.get(0).getSubject());
		
	}
	
}
