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
 * Servlet implementation class Datasend_NHIStoLINK
 */
@WebServlet("/J_NHIS/datasend_NHIStoLINK")
public class Datasend_NHIStoLINK extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public Datasend_NHIStoLINK() {
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
		
		System.err.println("Datasend_NHIStoLINK.java");
		

		request.setCharacterEncoding("utf-8");
		
		String nhisID = request.getParameter("NHISid");
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
//			System.out.println("(1)NHIS connect success");
//
//			
//			String sql = "SELECT * FROM nhis.nhis_data";
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
		
		
		// 연계 기관에서 nhis data 받을 테이블 생성

		try {

			con1 = JdbcUtil.getConnection();

			System.out.println("(1)LINK DB connect success");

			String sql = "CREATE TABLE link_take_nhis."+nhisID+"_"+tableName+"(linkID VARCHAR(255), STND_Y int(20), YKIHO_ID INT(20), YKIHO_GUBUN_CD INT(20), ORG_TYPE INT(20), YKIHO_SIDO INT(20), SICKBED_CNT INT(20), DR_CNT INT(20), CT_YN INT(20), MRI_YN INT(20), PET_YN INT(20), PRIMARY KEY(linkID))";

			pstmt1 = con1.prepareStatement(sql);
			pstmt1.executeUpdate();
			System.out.println("NHIS 연결번호 + 데이터 저장 테이블 생성 완료");

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
	
		
		// 연계 기관에서 nhis data 전송
		
		try {

			con2 = JdbcUtil.getConnection();

			String sql = "INSERT INTO link_take_nhis."+nhisID+"_"+tableName+" select nhis_take_indexer."+nhisID+"_"+tableName+".linkID, nhis.nhis_data.STND_Y, nhis.nhis_data.YKIHO_ID, nhis.nhis_data.YKIHO_GUBUN_CD, nhis.nhis_data.ORG_TYPE, nhis.nhis_data.YKIHO_SIDO, nhis.nhis_data.SICKBED_CNT,nhis.nhis_data.DR_CNT, nhis.nhis_data.CT_YN, nhis.nhis_data.MRI_YN, nhis.nhis_data.PET_YN FROM nhis_take_indexer."+nhisID+"_"+tableName+" INNER JOIN nhis.nhis_data ON nhis_take_indexer."+nhisID+"_"+tableName+".nhisID = nhis.nhis_data.nhis_ID";

			pstmt2 = con2.prepareStatement(sql);
			pstmt2.executeUpdate();
			
			System.out.println("NHIS 연결번호 + 데이터 전송 완료");

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
		
		
		// 연결번호 + 대조표 전송해줬으므로 nhis_take_data.nhis_take_data_info 에서 send_link를 1로 바꿔줌.
		
		try {

			con3 = JdbcUtil.getConnection();

			System.out.println("(1)NHIS DB connect success");

			String sql = "UPDATE nhis_take_data.nhis_take_data_info set send_link = 1 where tableName = '" + tableName + "'";

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
		
		
		// link_take_checklist.link_take_checklist_info 에 데이터 보내준 nhisID 입력
		
				try {

					con3 = JdbcUtil.getConnection();

					System.out.println("(2)nhis DB connect success");

					String sql = "UPDATE link_take_checklist.link_take_checklist_info set data_nhisID = '"+nhisID+"' where nhisTableName = '" + tableName + "'";

					pstmt3 = con3.prepareStatement(sql);
					pstmt3.executeUpdate();
					
					System.out.println("데이터 전송해준 nhisID 입력");

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
				
				
				// link_take_checklist.link_take_checklist_info 에 nhis_receive 1로 변경
				
				try {

					con3 = JdbcUtil.getConnection();

					System.out.println("(3)nhis DB connect success");

					String sql = "UPDATE link_take_checklist.link_take_checklist_info set nhis_receive = 1 where nhisTableName = '" + tableName + "'";

					pstmt3 = con3.prepareStatement(sql);
					pstmt3.executeUpdate();
					
					System.out.println("nhis_receive 1로 변경");

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
		
				
				RequestDispatcher rd = request.getRequestDispatcher("/J_NHIS/datasend_result_link.jsp");
				rd.forward(request, response);
				
				
		
	}

}
