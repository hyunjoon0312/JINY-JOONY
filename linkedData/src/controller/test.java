package controller;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;



import db.JdbcUtil;
import vo.MemberLINKNUM;

public class test {
public static void main(String[] args) {
	 Connection con2 = null;
     PreparedStatement pstmt2 = null;
     ResultSet rs = null;
     
		ArrayList<MemberLINKNUM> list = new ArrayList<MemberLINKNUM>();
		int listsize = 0;
		
		
		
		try {
			con2 = JdbcUtil.getConnection();
			System.out.println("(3)INDEXER connect success");	
			
			
			
			String sql = "SELECT * FROM indexer_checklist.nhistest_rtest_upload_sample_stattest_rtest_upload_sample";
			
			pstmt2 = con2.prepareStatement(sql);
			rs = pstmt2.executeQuery();
			
			while(rs.next()){
	
				int link_linkID = rs.getInt(1);
				String link_nhisID = rs.getString(2);
				String link_statID = rs.getString(3);
				
				vo.MemberLINKNUM memberLINKNUM = new vo.MemberLINKNUM(link_linkID, link_nhisID, link_statID);
				list.add(memberLINKNUM);
			}
			listsize = list.size();
			System.out.println("연결번호 리스트 불러옴");
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}finally {
			if(rs !=null){JdbcUtil.close(rs);}
			if(con2 != null){JdbcUtil.close(con2);}
			if(pstmt2 != null){JdbcUtil.close(pstmt2);}
		}
		
		System.out.println(list.get(0).getLinkID());
		
     
     
}
}
