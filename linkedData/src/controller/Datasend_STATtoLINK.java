package controller;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import db.JdbcUtil;

/**
 * Servlet implementation class Datasend_stattoLINK
 */
@WebServlet("/J_STAT/datasend_STATtoLINK")
public class Datasend_STATtoLINK extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public Datasend_STATtoLINK() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doPost(request, response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub

		
		System.err.println("Datasend_STATtoLINK.java");
		
		request.setCharacterEncoding("utf-8");
		
		String statID = request.getParameter("STATid");
		String tableName = request.getParameter("tableName"); 
		
		Connection con1 = null;
		Connection con2 = null;
		Connection con3 = null;
		
		
		PreparedStatement pstmt1 = null;
		PreparedStatement pstmt2 = null;
		PreparedStatement pstmt3 = null;
		
		ResultSet rs = null;
		ResultSetMetaData rsmd = null;
		
		
		
//		ArrayList<String> arrayList = new ArrayList<String>();
		
//		// 전체 DB명 가져오기
//		
//		try {
//
//			con1 = JdbcUtil.getConnection();
//
//			System.out.println("(1)stat connect success");
//
//			
//			String sql = "SELECT * FROM stat.stat_data";
//			pstmt1 = con1.prepareStatement(sql);
//			rs = pstmt1.executeQuery();
//			
//			rsmd = rs.getMetaData();
//			int numOfColumns = rsmd.getColumnCount();
//			System.out.println(numOfColumns);
//			
//			
//			for(int i = 1 ; i<= numOfColumns ; i++){
//				arrayList.add(rsmd.getColumnName(i));
//			}
//			
//			
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			if (con1 != null) {
//				JdbcUtil.close(con1);
//			}
//			if (rs != null) {
//				JdbcUtil.close(rs);
//			}
//		}
//		
		
		
		// 연계 기관에서 stat data 받을 테이블 생성

		try {

			con1 = JdbcUtil.getConnection();

			System.out.println("(1)LINK DB connect success");

			String sql = "CREATE TABLE link_take_stat."+statID+"_"+tableName+"(linkID VARCHAR(255), REPORT_YMD VARCHAR(20), ADDRESS INT(20), GENDER INT(20), DEATH_YMD VARCHAR(20), DEATH_TIME INT(20), DEATH_PLACE INT(20), DEATH_JOB VARCHAR(20), MARRY INT(20), EDU INT(20), DEATH_CAU1 VARCHAR(20), DEATH_CAU1_Parent VARCHAR(20), DEATH_AGE INT(20), PRIMARY KEY(linkID))";

			pstmt1 = con1.prepareStatement(sql);
			pstmt1.executeUpdate();
			System.out.println("STAT 암호화 연결번호 + 데이터 저장 테이블 생성 완료");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (con1 != null) {
				JdbcUtil.close(con1);
			}
			if (pstmt1 != null) {
				JdbcUtil.close(pstmt1);
			}
		}
	
		
		
		
		// 연계 기관에서 stat data 전송
		
		try {

			con2 = JdbcUtil.getConnection();

			String sql = "INSERT INTO link_take_stat."+statID+"_"+tableName+" select stat_take_indexer."+statID+"_"+tableName+".linkID, stat.stat_data.REPORT_YMD, stat.stat_data.ADDRESS, stat.stat_data.GENDER, stat.stat_data.DEATH_YMD, stat.stat_data.DEATH_TIME, stat.stat_data.DEATH_PLACE, stat.stat_data.DEATH_JOB, stat.stat_data.MARRY, stat.stat_data.EDU, stat.stat_data.DEATH_CAU1, stat.stat_data.DEATH_CAU1_Parent, stat.stat_data.DEATH_AGE FROM stat_take_indexer."+statID+"_"+tableName+" INNER JOIN stat.stat_data ON stat_take_indexer."+statID+"_"+tableName+".statID = stat.stat_data.stat_ID";

			pstmt2 = con2.prepareStatement(sql);
			pstmt2.executeUpdate();
			
			System.out.println("STAT 연결번호 + 데이터 전송 완료");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (con2 != null) {
				JdbcUtil.close(con2);
			}
			if (pstmt2 != null) {
				JdbcUtil.close(pstmt2);
			}
			
		}
		
		
		// 연결번호 + 대조표 전송해줬으므로 stat_take_data.stat_take_data_info 에서 send_link를 1로 바꿔줌.
		
		try {

			con3 = JdbcUtil.getConnection();

			System.out.println("(1)stat DB connect success");

			String sql = "UPDATE stat_take_data.stat_take_data_info set send_link = 1 where tableName = '" + tableName + "'";

			pstmt3 = con3.prepareStatement(sql);
			pstmt3.executeUpdate();
			
			System.out.println("send_link 1로 변경");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (con3 != null) {
				JdbcUtil.close(con3);
			}
			if (pstmt3 != null) {
				JdbcUtil.close(pstmt3);
			}
		}
		
		
		
		// link_take_checklist.link_take_checklist_info 에 데이터 보내준 statID 입력
		
		try {

			con3 = JdbcUtil.getConnection();

			System.out.println("(2)stat DB connect success");

			String sql = "UPDATE link_take_checklist.link_take_checklist_info set data_statID = '"+statID+"' where statTableName = '" + tableName + "'";

			pstmt3 = con3.prepareStatement(sql);
			pstmt3.executeUpdate();
			
			System.out.println("데이터 전송해준 statID 입력");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (con3 != null) {
				JdbcUtil.close(con3);
			}
			if (pstmt3 != null) {
				JdbcUtil.close(pstmt3);
			}
		}
		
		
		// link_take_checklist.link_take_checklist_info 에 stat_receive 1로 변경
		
				try {

					con3 = JdbcUtil.getConnection();

					System.out.println("(3)stat DB connect success");

					String sql = "UPDATE link_take_checklist.link_take_checklist_info set stat_receive = 1 where statTableName = '" + tableName + "'";

					pstmt3 = con3.prepareStatement(sql);
					pstmt3.executeUpdate();
					
					System.out.println("stat_receive 1로 변경");

				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					if (con3 != null) {
						JdbcUtil.close(con3);
					}
					if (pstmt3 != null) {
						JdbcUtil.close(pstmt3);
					}
				}
		
				
				
				RequestDispatcher rd = request.getRequestDispatcher("/J_STAT/datasend_result_link.jsp");
				rd.forward(request, response);
				
				
		
	}

}
