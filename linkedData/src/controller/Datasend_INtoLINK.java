package controller;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import db.JdbcUtil;

/**
 * Servlet implementation class datasend_IntoLINK
 */
@WebServlet("/J_IN/datasend_INtoLINK")
public class Datasend_INtoLINK extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public Datasend_INtoLINK() {
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
	
		request.setCharacterEncoding("utf-8");
		
	/*	String str__send = request.getParameter("checklist_send");
		int checklist_send = Integer.parseInt(str_checklist_send);*/
		String nhisTableName = request.getParameter("nhisTableName");
		String statTableName = request.getParameter("statTableName");
		String nhisID = request.getParameter("nhisID");
		String statID = request.getParameter("statID");
		String inID = request.getParameter("inID");
		String inName = request.getParameter("inName");
		String secretkey = request.getParameter("secretkey");
		
		
			
			Connection con1 = null;
			Connection con2 = null;
			Connection con3 = null;

			PreparedStatement pstmt1 = null;
			PreparedStatement pstmt2 = null;
			PreparedStatement pstmt3 = null;

			
			//indexer_take_data_info_secret 테이블에서 secretkey_send 1로 변경
			
			try {

				con1 = JdbcUtil.getConnection();

				System.out.println("(1)INDEXER_linknum DB connect success");

				String sql = "UPDATE indexer_linknum.indexer_take_data_info_secret set secretkey_send = 1 where secretkey = '"+secretkey+"'";

				pstmt1 = con1.prepareStatement(sql);
				pstmt1.executeUpdate();
				
				System.out.println("secretkey_send 1로 변경");

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
			
			
			
			
				//LINK에게 암호키 전달
			
			try {

				con1 = JdbcUtil.getConnection();

				String sql = "INSERT INTO link_secretKey.link_take_secretkey(secretKey, indexerID, nhisTableName, statTableName) VALUES('"+secretkey+"','"+inID+"','"+nhisTableName+"','"+statTableName+"')";

				pstmt1 = con1.prepareStatement(sql);
				pstmt1.executeUpdate();
				
				System.out.println("LINK에게 암호키 전송 완료");

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
			
			
			// 대조표 정보 저장 - 해당테이블 : link_take_checklist_info
			
			try {

				con1 = JdbcUtil.getConnection();

				String sql = "INSERT INTO link_take_checklist.link_take_checklist_info(indexerID, indexerName, checklist_tableName, nhisID, nhisTableName, statID, statTableName) VALUES(?,?,?,?,?,?,?)";

				pstmt1 = con1.prepareStatement(sql);
				
				pstmt1.setString(1, inID);
				pstmt1.setString(2, inName);
				pstmt1.setString(3, nhisID+"_"+nhisTableName+"_"+statID+"_"+statTableName);
				pstmt1.setString(4, nhisID);
				pstmt1.setString(5, nhisTableName);
				pstmt1.setString(6, statID);
				pstmt1.setString(7, statTableName);
				
				
				pstmt1.executeUpdate();
				
				System.out.println("대조표 정보 전송 완료");

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
			
			
			
			RequestDispatcher rd = request.getRequestDispatcher("/J_IN/Datasend_result_INtoLINK.jsp");
			rd.forward(request, response);
			
			
		
		
		
	}

}
