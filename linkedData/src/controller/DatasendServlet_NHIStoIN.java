package controller;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import db.JdbcUtil;
import db.JdbcUtilNHIS;
import db.JdbcUtilUpload;
import vo.MemberData;
import vo.MemberKEY_NHIS;
import vo.MemberSend;

/**
 * Servlet implementation class DatasendServlet
 */
@WebServlet("/J_NHIS/datasend_NHIStoIN")
public class DatasendServlet_NHIStoIN extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public DatasendServlet_NHIStoIN() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		// TODO Auto-generated method stub
		doPost(request, response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		// TODO Auto-generated method stub
		
		System.err.println("DatasendServelt_NHIStoIn");

		request.setCharacterEncoding("utf-8");
		
		String tableName = request.getParameter("tableName");
		String str_send_indexer = request.getParameter("send_indexer");
		String NHISid = request.getParameter("NHISid");
		String NHISname = request.getParameter("NHISname");
		int send_indexer = Integer.parseInt(str_send_indexer);

		HttpSession session = request.getSession();
		session.setAttribute("tableName", tableName);

		Connection con1 = null;
		Connection con2 = null;
		Connection con3 = null;
		Connection con4 = null;
		Connection con5 = null;

		PreparedStatement pstmt1 = null;
		PreparedStatement pstmt2 = null;
		PreparedStatement pstmt3 = null;
		PreparedStatement pstmt4 = null;
		PreparedStatement pstmt5 = null;

		ResultSet rs1 = null;

		if (send_indexer == 0) {

			try {

				con1 = JdbcUtil.getConnection();

				System.out.println("(1)NHIS DB connect success");

				String sql = "UPDATE nhis_take_data.nhis_take_data_info set send_indexer = 1 where tableName = " + "'"
						+ tableName + "'";

				pstmt1 = con1.prepareStatement(sql);
				pstmt1.executeUpdate();
				System.out.println("send_indexer 1로 변경");

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

			
			//-------- info --------
			
			try {
				con5 = JdbcUtil.getConnection();
				System.out.println("(2)NECA INDEXER DB connect success");

				String sql = "INSERT INTO indexer_take_nhis.indexer_take_nhis_info Values(DEFAULT,?,?,?,?)";

				pstmt5 = con5.prepareStatement(sql);


					pstmt5.setString(1, tableName);
					pstmt5.setString(2, NHISid);
					pstmt5.setString(3, NHISname);
					pstmt5.setInt(4, 0);
			
		
					
					pstmt5.executeUpdate();
				System.out.println("NHIS : INDEXER DB " + tableName + " 정보 데이터 저장 완료");
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			} finally {
				if (con5 != null) {
					JdbcUtil.close(con5);
				}
				if (pstmt5 != null) {
					JdbcUtil.close(pstmt5);
				}

			}

			
			
			// ArrayList 만들어서 데이터 리스트형태로 저장

			ArrayList<MemberKEY_NHIS> list = new ArrayList<MemberKEY_NHIS>();
			int listsize = 0;

			try {
				con2 = JdbcUtil.getConnection();
				System.out.println("(2)NHIS DB connect success");

				String sql = "SELECT nhis.nhis_data.PERSON_ID, nhis.nhis_data.nhis_ID FROM nhis.nhis_data INNER JOIN nhis_take_data."
						+ tableName + " ON nhis.nhis_data.PERSON_ID = nhis_take_data." + tableName + ".personID";

				pstmt2 = con2.prepareStatement(sql);
				rs1 = pstmt2.executeQuery();

				while (rs1.next()) {

					String personID = rs1.getString(1);
					String nhisID = rs1.getString(2);
					MemberKEY_NHIS memberKEY_NHIS = new MemberKEY_NHIS(personID, nhisID);
					list.add(memberKEY_NHIS);
				}
				listsize = list.size();
				System.out.println(tableName + "식별자와 해당 NHIS 식별자 리스트로 불러옴");
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			} finally {
				if (rs1 != null) {
					JdbcUtil.close(rs1);
				}
				if (con2 != null) {
					JdbcUtil.close(con2);
				}
				if (pstmt2 != null) {
					JdbcUtil.close(pstmt2);
				}
			}

			// 리스트형태로 불러온 데이터 저장할 테이블 생성

			try {
				con3 = JdbcUtil.getConnection();
				System.out.println("(1)NECA INDEXER DB connect success");

				String sql = "CREATE TABLE indexer_take_nhis." + tableName
						+ " (personID VARCHAR(20), nhisID VARCHAR(20), PRIMARY KEY(personID))";

				pstmt3 = con3.prepareStatement(sql);
				pstmt3.executeUpdate();

				System.out.println("NECA INDEXER DB에 " + tableName + " 테이블 생성");
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			} finally {
				if (con3 != null) {
					JdbcUtil.close(con3);
				}
				if (pstmt3 != null) {
					JdbcUtil.close(pstmt3);
				}

			}

			// 리스트형태로 불러온 데이터 저장

			try {
				con4 = JdbcUtil.getConnection();
				System.out.println("(2)NECA INDEXER DB connect success");

				// 나중에 내 방식대로 할 때 필요한 코드
				String sql = "INSERT INTO indexer_take_nhis." + tableName + " Values(?,?)";

				pstmt4 = con4.prepareStatement(sql);

				for (int i = 0; i < listsize; i++) {

					pstmt4.setString(1, list.get(i).getpersonID());
					pstmt4.setString(2, list.get(i).getnhisID());
					pstmt4.addBatch();

				}

				int[] cnt = pstmt4.executeBatch();
				System.out.println(cnt.length + "row 성공");
				System.out.println("INDEXER DB " + tableName + " 테이블에 데이터 저장 완료");
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			} finally {
				if (con4 != null) {
					JdbcUtil.close(con4);
				}
				if (pstmt4 != null) {
					JdbcUtil.close(pstmt4);
				}

			}

		}

		RequestDispatcher rd = request.getRequestDispatcher("/J_NHIS/Datasend_result_NHIStoIN.jsp");
		rd.forward(request, response);

	}

}
