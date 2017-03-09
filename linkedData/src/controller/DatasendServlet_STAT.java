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
import db.JdbcUtilSTAT;
import db.JdbcUtilUpload;
import vo.MemberData;
import vo.MemberSend;


/**
 * Servlet implementation class DatasendServlet
 */
@WebServlet("/J_NEOK/datasend_stat")
public class DatasendServlet_STAT extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public DatasendServlet_STAT() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doPost(request,response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		
		
		System.err.println("DatasendServlet_STAT");
		
		request.setCharacterEncoding("utf-8");
		
		String filename = request.getParameter("filename");
		String uploadername = request.getParameter("uploadername");
		String uploaderid = request.getParameter("uploaderid");
		String NEOKid = request.getParameter("NEOKid");
		String NEOKname = request.getParameter("NEOKname");
		String str_stat_send = request.getParameter("stat_send");
		int stat_send = Integer.parseInt(str_stat_send);
		
		
		MemberSend memberSend = new MemberSend();
		
		
		HttpSession session = request.getSession();
		session.setAttribute("filename", filename);
		session.setAttribute("uploadername", uploadername);
		session.setAttribute("uploaderid", uploaderid);


		String refilename = null;
		refilename = filename.replace(".csv", "");
		
		Connection con1 = null;
		Connection con2 = null;
		Connection con3 = null;
		Connection con4 = null;
		Connection con5 = null;
		Connection con6 = null;
		Connection con7 = null;
		Connection con8 = null;
		Connection con9 = null;
		Connection con10 = null;
		PreparedStatement pstmt1 = null;
		PreparedStatement pstmt2 = null;
		PreparedStatement pstmt3 = null;
		PreparedStatement pstmt4 = null;
		PreparedStatement pstmt5 = null;
		PreparedStatement pstmt6 = null;
		PreparedStatement pstmt7 = null;
		PreparedStatement pstmt8 = null;
		PreparedStatement pstmt9 = null;
		PreparedStatement pstmt10 = null;
		ResultSet rs1 = null;
		ResultSet rs2 = null;
		ResultSet rs3 = null;
		ResultSet rs4 = null;
		
		/*try{
			
			Class.forName("com.mysql.jdbc.Driver");
			con = JdbcUtilUpload.getUploadConnection();
					
			System.out.println("UploadDB connect success");
		
			stmt = con.createStatement();
			
			String sql = "SELECT stat_send FROM uploadFile.UploadFileInfo where filename = "+filename;
		
			rs = stmt.executeQuery(sql);
		
			while(rs.next()){
				stat_send = rs.getInt("stat_send");
			}
		
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			JdbcUtilUpload.close(con);
			
			try {
				stmt.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			JdbcUtilUpload.close(rs);
		}*/
		
		
		
		// stat_send == 0 이면 데이터 읽어와서 nhis 기관 DB에 저장
		
		if(stat_send == 0){

			// stat_send 를 1로 변경

		try{
			
			con1 = JdbcUtilUpload.getUploadConnection();
					
			System.out.println("(1)UploadDB connect success");
		
		
			String sql = "UPDATE uploadFile.UploadFileInfo set stat_send = 1 where filename = "+"'"+filename+"'";
			
		
			pstmt1 = con1.prepareStatement(sql);
			pstmt1.executeUpdate();
			System.out.println("stat_send 1로 변경");
		
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(con1 != null){JdbcUtilUpload.close(con1);}
			if(pstmt1 != null){JdbcUtilUpload.close(pstmt1);}
		}
		
		// ArrayList 만들어서 데이터 리스트형태로 저장
		
		ArrayList<MemberData> list = new ArrayList<MemberData>();
		int listsize = 0;
		
		
		try {
			con2 = JdbcUtilUpload.getUploadConnection();
			System.out.println("(2)UploadDB connect success");	
			
			
			
			String sql = "SELECT * FROM uploadFile."+uploaderid+"_"+refilename;
			
			pstmt2 = con2.prepareStatement(sql);
			rs1 = pstmt2.executeQuery();
			
			while(rs1.next()){
				//나중에 내 방식대로 할때 필요한 코드
				/*int linkID = rs.getInt(1);
				String personID = rs.getString(2);*/
				
				String personID = rs1.getString(1);
				//MemberData memberData = new MemberData(linkID, personID);
				MemberData memberData = new MemberData(personID);
				list.add(memberData);
			}
			listsize = list.size();
			System.out.println(uploaderid+"_"+refilename+" 데이터 리스트로 불러옴");
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}finally {
			if(rs1 !=null){JdbcUtilUpload.close(rs1);}
			if(con2 != null){JdbcUtilUpload.close(con2);}
			if(pstmt2 != null){JdbcUtilUpload.close(pstmt2);}
		}
		
		
		// 리스트형태로 불러온 데이터 저장할 테이블 새성
		
		
		try {
			con3 = JdbcUtilSTAT.getSTATConnection();
			System.out.println("(1)stat_send DB connect success");
			
			//나중에 내 방식대로 할 때 필요한 쿼리
			//String sql = "CREATE TABLE "+uploadername+"_"+refilename+" (linkID int(20), personID VARCHAR(20), PRIMARY KEY(linkID))";
			String sql = "CREATE TABLE "+uploaderid+"_"+refilename+" (personID VARCHAR(20), PRIMARY KEY(personID))";
			
			pstmt3 = con3.prepareStatement(sql);
			pstmt3.executeUpdate();
			
			System.out.println(uploaderid+"_"+refilename+"테이블 생성");
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}finally {
			if(con3 != null){JdbcUtilSTAT.close(con3);}
			if(pstmt3 != null){JdbcUtilSTAT.close(pstmt3);}
			
		}
		
		
	
		
		
		// 리스트형태로 불러온 데이터 저장
		
		try {
			con6 = JdbcUtilSTAT.getSTATConnection();
			System.out.println("(3)stat_send DB connect success");
			
			//나중에 내방식대로 할때 필요한 코드
			//String sql = "INSERT INTO "+uploadername+"_"+refilename+" Values(?,?)";
			String sql = "INSERT INTO "+uploaderid+"_"+refilename+" Values(?)";
			
			pstmt6 = con6.prepareStatement(sql);
			
			for(int i =0;i<listsize;i++){
				
				
				pstmt6.setString(1, list.get(i).getPersonID());
				pstmt6.addBatch();
			
			
			//나중에 내 방식대로 할때 필요한 코드
			/*	pstmt6.setInt(1, list.get(i).getLinkID());
				pstmt6.setString(2, list.get(i).getPersonID());
				pstmt6.addBatch();*/
				
				
			}
			
			int[] cnt = pstmt6.executeBatch();
			System.out.println(cnt.length+"row 성공");
			System.out.println(uploaderid+"_"+refilename+" 데이터 저장 완료");
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}finally {
			if(con6 != null){JdbcUtilSTAT.close(con6);}
			if(pstmt6 != null){JdbcUtilSTAT.close(pstmt6);}
			
		}
		
		
		}
		
		
		//-------- 각 기관에 데이터 전송하였는지 여부 확인 -----------------------
		
		try {
			con7 = JdbcUtilUpload.getUploadConnection();
			System.out.println("(3)UploadDB connect success");	
			
			
			
			String sql = "SELECT nhis_send, stat_send FROM uploadFile.UploadFileInfo where filename = "+"'"+filename+"'";
			
			
			pstmt7 = con7.prepareStatement(sql);
			rs2 = pstmt7.executeQuery();
			
			while(rs2.next()){
				memberSend = new MemberSend();
				memberSend.setNhis_send(rs2.getInt(1)); 
				memberSend.setStat_send(rs2.getInt(2));
	
			}
		
			System.out.println(filename+" 데이터 기관 전송 여부 확인");
			
			
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}finally {
			if(rs2 !=null){JdbcUtilUpload.close(rs2);}
			if(con7 != null){JdbcUtilUpload.close(con7);}
			if(pstmt7 != null){JdbcUtilUpload.close(pstmt7);}
		}
		

		//----------- 각 기관에 데이터 전송 여부에 따른 진행 ----------
		
		if(memberSend.getNhis_send() == 1 && memberSend.getStat_send() == 1){
			
			try{
				
				con8 = JdbcUtilUpload.getUploadConnection();
						
				System.out.println("(4)UploadDB connect success");
			
			
				String sql = "UPDATE uploadFile.UploadFileInfo set send_ok = 1 where filename = "+"'"+filename+"'";
				
			
				pstmt8 = con8.prepareStatement(sql);
				pstmt8.executeUpdate();
				System.out.println("send_ok 1로 변경");
			
			}catch(Exception e){
				e.printStackTrace();
			}finally{
				if(con8 != null){JdbcUtilUpload.close(con8);}
				if(pstmt8 != null){JdbcUtilUpload.close(pstmt8);}
			}
			
			
			
			
			try{
				
				con9 = JdbcUtilUpload.getUploadConnection();
						
				System.out.println("(5)UploadDB connect success");
			
			
				String sql = "DROP TABLE uploadFile."+uploaderid+"_"+refilename;
				
			
				pstmt9 = con9.prepareStatement(sql);
				pstmt9.executeUpdate();
				System.out.println(uploaderid+"_"+refilename+" 테이블 삭제");
			
			}catch(Exception e){
				e.printStackTrace();
			}finally{
				if(con9 != null){JdbcUtilUpload.close(con9);}
				if(pstmt9 != null){JdbcUtilUpload.close(pstmt9);}
			}
			
			
			
			
			//요청하는 데이터의 row 수 계산
			

			try {
				con4 = JdbcUtil.getConnection();
				System.out.println("(3)UploadDB connect success");	
				
				
				
				String sql = "SELECT count(*) FROM stat_take_data."+uploaderid+"_"+refilename;
				
				System.out.println(sql);
				
				pstmt4 = con4.prepareStatement(sql);
				rs3 = pstmt4.executeQuery();
				
				while(rs3.next()){
					memberSend.setRequest_row(rs3.getInt(1));
					System.err.println("요청row"+rs3.getInt(1));
		
				}
			
				System.out.println("요청 데이터 총 row 수 : "+memberSend.getRequest_row());
				
				
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}finally {
				if(rs3 !=null){JdbcUtilUpload.close(rs3);}
				if(con4 != null){JdbcUtilUpload.close(con4);}
				if(pstmt4 != null){JdbcUtilUpload.close(pstmt4);}
			}
			
			
			//일치하는 row 수 계산

			try {
				con10 = JdbcUtil.getConnection();
				System.out.println("statDB connect success");	
				
				
				
				String sql = "SELECT count(stat.stat_data.PERSON_ID) FROM stat.stat_data INNER JOIN stat_take_data."+uploaderid+"_"+refilename+" ON stat.stat_data.PERSON_ID = stat_take_data."+uploaderid+"_"+refilename+".personID";
				
				
				pstmt10 = con10.prepareStatement(sql);
				rs4 = pstmt10.executeQuery();
				
				while(rs4.next()){
					memberSend.setAvailable_row(rs4.getInt(1));
					System.err.println("일치 row"+rs4.getInt(1));
				}
			
				System.out.println("일치하는 총 row 수 : "+memberSend.getAvailable_row());
				
				
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}finally {
				if(rs4 !=null){JdbcUtilUpload.close(rs4);}
				if(con10 != null){JdbcUtilUpload.close(con10);}
				if(pstmt10 != null){JdbcUtilUpload.close(pstmt10);}
			}
			
			
			
			
			
			
			// 식별자 info 테이블에 데이터 저장
			
					try {
						con5 = JdbcUtilSTAT.getSTATConnection();
						System.out.println("(2)stat_send DB connect success");
						
						//나중에 내 방식대로 할 때 필요한 코드
						//String sql = "INSERT INTO "+uploadername+"_"+refilename+" Values(?,?)";
						String sql = "INSERT INTO stat_take_data.stat_take_data_info Values(default,?,?,?,?,?,?,?,?)";
						
						pstmt5 = con5.prepareStatement(sql);
						
						pstmt5.setString(1, uploaderid+"_"+refilename);
						pstmt5.setString(2, NEOKid);
						pstmt5.setString(3, NEOKname);
						pstmt5.setInt(4, memberSend.getRequest_row());
						pstmt5.setInt(5, memberSend.getAvailable_row());
						pstmt5.setInt(6, 0);
						pstmt5.setInt(7, 0);
						pstmt5.setInt(8, 0);
						
						//나중에 내 방식대로 할때 필요한 코드
						/*	pstmt6.setInt(1, list.get(i).getLinkID());
							pstmt6.setString(2, list.get(i).getPersonID());
							pstmt6.addBatch();*/
							
						pstmt5.executeUpdate();	

						
						
						System.out.println(uploaderid+"_"+refilename+" 테이블 정보 저장 완료");
					} catch (Exception e) {
						// TODO: handle exception
						e.printStackTrace();
					}finally {
						if(con5 != null){JdbcUtilSTAT.close(con5);}
						if(pstmt5 != null){JdbcUtilSTAT.close(pstmt5);}
						
					}
			
			
			
			
		}
		
		
		RequestDispatcher rd = request.getRequestDispatcher("/J_NEOK/datasend_result_stat.jsp");
		rd.forward(request, response);
		
		
	}

}
