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
import svc.CreateSecertKey;

/**
 * Servlet implementation class Create_secretkey
 */
@WebServlet("/J_IN/create_secretkey")
public class Create_secretkey extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public Create_secretkey() {
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

		System.err.println("Create_secretkey.java");
		
		request.setCharacterEncoding("utf-8");

		// 암호화 키 생성 및 저장

					String secretkey = null;
					String nhisTableName = request.getParameter("nhisTableName");
					String statTableName = request.getParameter("statTableName");
					
					
					Connection con = null;
					PreparedStatement pstmt =null;
					ResultSet rs =null;
					
					
					try {

						
						CreateSecertKey CSK = new CreateSecertKey();
						
						secretkey = CSK.getRandomString(25);
						
						
						con = JdbcUtil.getConnection();

						System.out.println("(1)INDEXER DB connect success");

						String sql = "UPDATE indexer_linknum.indexer_take_data_info_secret set secretkey = '"+secretkey+"' WHERE nhisTableName = '"+nhisTableName+"' and statTableName = '"+statTableName+"'";

						pstmt = con.prepareStatement(sql);
						pstmt.executeUpdate();
						System.out.println("암호키 생성 및 저장");

					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						if (con != null) {
							JdbcUtil.close(con);
						}
						if (pstmt != null) {
							JdbcUtil.close(pstmt);
						}
					}
	
					
					try {

						
						con = JdbcUtil.getConnection();

						System.out.println("(2)INDEXER DB connect success");

						String sql = "UPDATE indexer_linknum.indexer_take_data_info_secret set create_secretkey = 1 WHERE secretkey = '"+secretkey+"'";

						pstmt = con.prepareStatement(sql);
						pstmt.executeUpdate();
						System.out.println("creat_secretkey 1로 변경");

					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						if (con != null) {
							JdbcUtil.close(con);
						}
						if (pstmt != null) {
							JdbcUtil.close(pstmt);
						}
					}
					
					
					
					
					RequestDispatcher rd = request.getRequestDispatcher("/J_IN/Create_secretkey_result.jsp");
					rd.forward(request, response);
					
					
					
	}

}
