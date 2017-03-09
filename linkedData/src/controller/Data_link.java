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


import db.JdbcUtil;
import db.JdbcUtilUpload;
import svc.AES256Util;
import vo.MemberLINKNUM;
import vo.MemberLinkSecretkey;
import vo.MemberNhisData;
import vo.MemberSend;
import vo.MemberStatData;

/**
 * Servlet implementation class Data_link
 */
@WebServlet("/J_LINK/data_link")
public class Data_link extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public Data_link() {
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
		
		System.err.println("Data_link.java");
		
		
		request.setCharacterEncoding("utf-8");
		
		
		String nhisID = request.getParameter("nhisID");
		String nhisTableName = request.getParameter("nhisTableName");
		String statID = request.getParameter("statID");
		String statTableName = request.getParameter("statTableName");
		
		
		Connection con1 = null;
		Connection con2 = null;
		Connection con3 = null;
		
		PreparedStatement pstmt1 = null;
		PreparedStatement pstmt2 = null;
		PreparedStatement pstmt3 = null;
		
		ResultSet rs = null;
		
		
		// 암호키 가져옴
		
		String secretkey = null;
		
		
		try {
			con1 = JdbcUtil.getConnection();
			System.out.println("(1)LINK connect success");	
			
			
			
			String sql = "SELECT * FROM link_secretKey.link_take_secretkey WHERE nhisTableName = '"+nhisTableName+"' and statTableName = '"+statTableName+"'";
			
			
			pstmt1 = con1.prepareStatement(sql);
			rs = pstmt1.executeQuery();
			
			while(rs.next()){
				MemberLinkSecretkey memberLinkSecretkey = new MemberLinkSecretkey();
				memberLinkSecretkey.setSecreKey(rs.getString(2)); 
				memberLinkSecretkey.setIndexerID(rs.getString(3)); 
				memberLinkSecretkey.setNhisTableName(rs.getString(4)); 
				memberLinkSecretkey.setStatTableName(rs.getString(5));
				
				secretkey = memberLinkSecretkey.getSecreKey();
	
			}
		
			System.out.println("암호키 정보 읽어옴");
			
			
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}finally {
			if(rs !=null){JdbcUtil.close(rs);}
			if(con1 != null){JdbcUtil.close(con1);}
			if(pstmt1 != null){JdbcUtil.close(pstmt1);}
		}
		
		
		//복화화된 연결번호가 있는 데이터를 담을 테이블 생성
		
		//NHIS
		
		
		try {

			con1 = JdbcUtil.getConnection();

			System.out.println("(1)LINK DB connect success");

			
			String sql = "CREATE TABLE link_take_nhis_dec."+nhisID+"_"+nhisTableName+"(linkID int(20), STND_Y int(20), YKIHO_ID INT(20), YKIHO_GUBUN_CD INT(20), ORG_TYPE INT(20), YKIHO_SIDO INT(20), SICKBED_CNT INT(20), DR_CNT INT(20), CT_YN INT(20), MRI_YN INT(20), PET_YN INT(20), PRIMARY KEY(linkID))";

			pstmt1 = con1.prepareStatement(sql);
			pstmt1.executeUpdate();
			System.out.println("복호화 nhis 테이블 생성 완료");

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
		
		
		
		
		// STAT
		try {

			con1 = JdbcUtil.getConnection();

			System.out.println("(1)LINK DB connect success");

			String sql = "CREATE TABLE link_take_stat_dec."+statID+"_"+statTableName+"(linkID int(20), REPORT_YMD VARCHAR(20), ADDRESS INT(20), GENDER INT(20), DEATH_YMD VARCHAR(20), DEATH_TIME INT(20), DEATH_PLACE INT(20), DEATH_JOB VARCHAR(20), MARRY INT(20), EDU INT(20), DEATH_CAU1 VARCHAR(20), DEATH_CAU1_Parent VARCHAR(20), DEATH_AGE INT(20), PRIMARY KEY(linkID))";

			pstmt1 = con1.prepareStatement(sql);
			pstmt1.executeUpdate();
			System.out.println("복호화 stat 테이블 생성 완료");

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
		
		
		
		
		// 복호화를 위해 암호화 연결번호 테이블 읽어오기
		
		
		ArrayList<MemberNhisData> nhis_list = new ArrayList<MemberNhisData>();
		int nhis_listsize = 0;
		
		
		try {
			con2 = JdbcUtil.getConnection();
			System.out.println("(2)LINK connect success");	
			
			
			
			String sql = "SELECT * FROM link_take_nhis."+nhisID+"_"+nhisTableName;
			
			pstmt2 = con2.prepareStatement(sql);
			rs = pstmt2.executeQuery();
			
			while(rs.next()){
	
				String sec_linkID = rs.getString(1);
				int STND_Y = rs.getInt(2);
				int YKIHO_ID = rs.getInt(3);
				int YKIHO_GUBUN_CD = rs.getInt(4);
				int ORG_TYPE = rs.getInt(5);
				int YKIHO_SIDO = rs.getInt(6);
				int SICKBED_CNT = rs.getInt(7);
				int DR_CNT = rs.getInt(8);
				int CT_YN = rs.getInt(9);
				int MRI_YN = rs.getInt(10);
				int PET_YN = rs.getInt(11);
				
				MemberNhisData memberNhisData = new MemberNhisData(sec_linkID, STND_Y, YKIHO_ID, YKIHO_GUBUN_CD, ORG_TYPE, YKIHO_SIDO, SICKBED_CNT, DR_CNT, CT_YN, MRI_YN, PET_YN);
				nhis_list.add(memberNhisData);
			}
			nhis_listsize = nhis_list.size();
			System.out.println("NHIS 암호화 된 연결번호 리스트 불러옴");
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}finally {
			if(rs !=null){JdbcUtil.close(rs);}
			if(con2 != null){JdbcUtil.close(con2);}
			if(pstmt2 != null){JdbcUtil.close(pstmt2);}
		}
		
		
		//암호화된 연결번호를 가진 STAT 데이터 불러와야함.
		
		
		ArrayList<MemberStatData> stat_list = new ArrayList<MemberStatData>();
		int stat_listsize = 0;
		
		
		try {
			con2 = JdbcUtil.getConnection();
			System.out.println("(2)LINK connect success");	
			
			
			
			String sql = "SELECT * FROM link_take_stat."+statID+"_"+statTableName;
			
			pstmt2 = con2.prepareStatement(sql);
			rs = pstmt2.executeQuery();
			
			while(rs.next()){
	
				String sec_linkID = rs.getString(1);
				String REPORT_YMD = rs.getString(2);
				int ADDRESS = rs.getInt(3);
				int GENDER = rs.getInt(4);
				String DEATH_YMD = rs.getString(5);
				int DEATH_TIME = rs.getInt(6);
				int DEATH_PLACE = rs.getInt(7);
				String DEATH_JOB = rs.getString(8);
				int MARRY = rs.getInt(9);
				int EDU = rs.getInt(10);
				String DEATH_CAU1 = rs.getString(11);
				String DEATH_CAU1_Parent = rs.getString(12);
				int DEATH_AGE = rs.getInt(13);
				
				MemberStatData memberStatData = new MemberStatData(sec_linkID, REPORT_YMD, ADDRESS, GENDER, DEATH_YMD, DEATH_TIME, DEATH_PLACE, DEATH_JOB, MARRY, EDU, DEATH_CAU1, DEATH_CAU1_Parent, DEATH_AGE);
				stat_list.add(memberStatData);
			}
			stat_listsize = stat_list.size();
			System.out.println("STAT 암호화 된 연결번호 리스트 불러옴");
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}finally {
			if(rs !=null){JdbcUtil.close(rs);}
			if(con2 != null){JdbcUtil.close(con2);}
			if(pstmt2 != null){JdbcUtil.close(pstmt2);}
		}
		
		
		//복호화해서 저장 
		
		//NHIS
		
		AES256Util aes256 = new AES256Util(secretkey);
		
		try {

			
			con2 = JdbcUtil.getConnection();

			String sql = "INSERT INTO link_take_nhis_dec."+nhisID+"_"+nhisTableName+"(linkID, STND_Y , YKIHO_ID , YKIHO_GUBUN_CD , ORG_TYPE, YKIHO_SIDO, SICKBED_CNT, DR_CNT, CT_YN, MRI_YN, PET_YN) VALUES(?,?,?,?,?,?,?,?,?,?,?)";

			pstmt2 = con2.prepareStatement(sql);
			
			
			for(int i = 0 ; i<nhis_listsize; i++){
			pstmt2.setInt(1, Integer.parseInt(aes256.aesDecode(String.valueOf(nhis_list.get(i).getLinkID()))));
			pstmt2.setInt(2, nhis_list.get(i).getSTND_Y());
			pstmt2.setInt(3, nhis_list.get(i).getYKIHO_ID());
			pstmt2.setInt(4, nhis_list.get(i).getYKIHO_GUBUN_CD());
			pstmt2.setInt(5, nhis_list.get(i).getORG_TYPE());
			pstmt2.setInt(6, nhis_list.get(i).getYKIHO_SIDO());
			pstmt2.setInt(7, nhis_list.get(i).getSICKBED_CNT());
			pstmt2.setInt(8, nhis_list.get(i).getDR_CNT());
			pstmt2.setInt(9, nhis_list.get(i).getCT_YN());
			pstmt2.setInt(10, nhis_list.get(i).getMRI_YN());
			pstmt2.setInt(11, nhis_list.get(i).getPET_YN());
			pstmt2.addBatch();
			
			}
			int[] cnt = pstmt2.executeBatch();
			
			System.out.println("nhis 복호화 후 데이터 저장 완료");

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("err:" + e.toString());
		} finally {
			if (con2 != null) {
				JdbcUtil.close(con2);
			}
			if (pstmt2 != null) {
				JdbcUtil.close(pstmt2);
			}
			
		}
		
		//STAT
		
		
			try {

				con2 = JdbcUtil.getConnection();

				String sql = "INSERT INTO link_take_stat_dec."+statID+"_"+statTableName+"(linkID, REPORT_YMD, ADDRESS, GENDER, DEATH_YMD, DEATH_TIME, DEATH_PLACE, DEATH_JOB, MARRY, EDU, DEATH_CAU1, DEATH_CAU1_Parent, DEATH_AGE) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";

				pstmt2 = con2.prepareStatement(sql);
				
				for(int i = 0 ; i<stat_listsize; i++){
					pstmt2.setInt(1, Integer.parseInt(aes256.aesDecode(String.valueOf(stat_list.get(i).getLinkID()))));
					pstmt2.setString(2, stat_list.get(i).getREPORT_YMD());
					pstmt2.setInt(3, stat_list.get(i).getADDRESS());
					pstmt2.setInt(4, stat_list.get(i).getGENDER());
					pstmt2.setString(5, stat_list.get(i).getDEATH_YMD());
					pstmt2.setInt(6, stat_list.get(i).getDEATH_TIME());
					pstmt2.setInt(7, stat_list.get(i).getDEATH_PLACE());
					pstmt2.setString(8, stat_list.get(i).getDEATH_JOB());
					pstmt2.setInt(9, stat_list.get(i).getMARRY());
					pstmt2.setInt(10, stat_list.get(i).getEDU());
					pstmt2.setString(11, stat_list.get(i).getDEATH_CAU1());
					pstmt2.setString(12, stat_list.get(i).getDEATH_CAU1_Parent());
					pstmt2.setInt(13, stat_list.get(i).getDEATH_AGE());
					pstmt2.addBatch();
					
					}
					int[] cnt = pstmt2.executeBatch();
				System.out.println("stat 복호화 후 데이터 저장 완료");

			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("err:" + e.toString());
			} finally {
				if (con2 != null) {
					JdbcUtil.close(con2);
				}
				if (pstmt2 != null) {
					JdbcUtil.close(pstmt2);
				}
				
			}
		
		
		
		// link_safe_DB 에 테이블 생성
		
		try {

			con1 = JdbcUtil.getConnection();

			System.out.println("(1)LINK DB connect success");

			String sql = "CREATE TABLE link_safe_DB."+nhisID+"_"+nhisTableName+"_"+statID+"_"+statTableName+"(NUM int(20) auto_increment, STND_Y int(20), YKIHO_ID INT(20), YKIHO_GUBUN_CD INT(20), ORG_TYPE INT(20), YKIHO_SIDO INT(20), SICKBED_CNT INT(20), DR_CNT INT(20), CT_YN INT(20), MRI_YN INT(20), PET_YN INT(20), REPORT_YMD VARCHAR(20), ADDRESS INT(20), GENDER INT(20), DEATH_YMD VARCHAR(20), DEATH_TIME INT(20), DEATH_PLACE INT(20), DEATH_JOB VARCHAR(20), MARRY INT(20), EDU INT(20), DEATH_CAU1 VARCHAR(20), DEATH_CAU1_Parent VARCHAR(20), DEATH_AGE INT(20), PRIMARY KEY(NUM))";

			pstmt1 = con1.prepareStatement(sql);
			pstmt1.executeUpdate();
			System.out.println("연계 데이터 저장 테이블 생성 완료");

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
		
		
		
		//데이터 JOIN 후 Input
		
		try {

			con2 = JdbcUtil.getConnection();

			String sql = "INSERT INTO link_safe_DB."+nhisID+"_"+nhisTableName+"_"+statID+"_"+statTableName+"(STND_Y , YKIHO_ID , YKIHO_GUBUN_CD , ORG_TYPE, YKIHO_SIDO, SICKBED_CNT, DR_CNT, CT_YN, MRI_YN, PET_YN, REPORT_YMD, ADDRESS, GENDER, DEATH_YMD, DEATH_TIME, DEATH_PLACE, DEATH_JOB, MARRY, EDU, DEATH_CAU1, DEATH_CAU1_Parent, DEATH_AGE) SELECT link_take_nhis_dec."+nhisID+"_"+nhisTableName+".STND_Y,"+" link_take_nhis_dec."+nhisID+"_"+nhisTableName+".YKIHO_ID,"+" link_take_nhis_dec."+nhisID+"_"+nhisTableName+".YKIHO_GUBUN_CD, "+"link_take_nhis_dec."+nhisID+"_"+nhisTableName+".ORG_TYPE, "+"link_take_nhis_dec."+nhisID+"_"+nhisTableName+".YKIHO_SIDO, "+"link_take_nhis_dec."+nhisID+"_"+nhisTableName+".SICKBED_CNT, "+"link_take_nhis_dec."+nhisID+"_"+nhisTableName+".DR_CNT, "+"link_take_nhis_dec."+nhisID+"_"+nhisTableName+".CT_YN, "+"link_take_nhis_dec."+nhisID+"_"+nhisTableName+".MRI_YN, "+"link_take_nhis_dec."+nhisID+"_"+nhisTableName+".PET_YN, link_take_stat_dec."+statID+"_"+statTableName+".REPORT_YMD, "+"link_take_stat_dec."+statID+"_"+statTableName+".ADDRESS, "+"link_take_stat_dec."+statID+"_"+statTableName+".GENDER, "+"link_take_stat_dec."+statID+"_"+statTableName+".DEATH_YMD, "+"link_take_stat_dec."+statID+"_"+statTableName+".DEATH_TIME, "+"link_take_stat_dec."+statID+"_"+statTableName+".DEATH_PLACE, "+"link_take_stat_dec."+statID+"_"+statTableName+".DEATH_JOB, "+"link_take_stat_dec."+statID+"_"+statTableName+".MARRY, "+"link_take_stat_dec."+statID+"_"+statTableName+".EDU, "+"link_take_stat_dec."+statID+"_"+statTableName+".DEATH_CAU1, "+"link_take_stat_dec."+statID+"_"+statTableName+".DEATH_CAU1_Parent, "+"link_take_stat_dec."+statID+"_"+statTableName+".DEATH_AGE"+" FROM link_take_nhis_dec."+nhisID+"_"+nhisTableName+" INNER JOIN link_take_stat_dec."+statID+"_"+statTableName+" ON link_take_nhis_dec."+nhisID+"_"+nhisTableName+".linkID = link_take_stat_dec."+statID+"_"+statTableName+".linkID";

			pstmt2 = con2.prepareStatement(sql);
			pstmt2.executeUpdate();
			
			System.out.println("데이터 연계 완료");

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("err:" + e.toString());
		} finally {
			if (con2 != null) {
				JdbcUtil.close(con2);
			}
			if (pstmt2 != null) {
				JdbcUtil.close(pstmt2);
			}
			
		}
		
		
		
		// 데이터 연계해줬으니까 link_take_checklist.link_take_checklist_info 에서 data_link 1로 변경 해줌.
		
		try {

			con3 = JdbcUtil.getConnection();

			System.out.println("(2)LINK DB connect success");

			String sql = "UPDATE link_take_checklist.link_take_checklist_info set data_link = 1 where checklist_tableName = '" +nhisID+"_"+nhisTableName+"_"+statID+"_"+statTableName+ "'";

			pstmt3 = con3.prepareStatement(sql);
			pstmt3.executeUpdate();
			
			System.out.println("data_link 1로 변경");

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
		
		
		// uploadFile.UploadgFileInfo 에도 데이터 연계 했다고 알려줌.
		
		try {

			con3 = JdbcUtil.getConnection();

			System.out.println("(1)uploadFile DB connect success");

			String sql = "UPDATE uploadFile.UploadFileInfo set data_link = 1 where id_filename= '"+nhisTableName+ ".csv'";

			pstmt3 = con3.prepareStatement(sql);
			pstmt3.executeUpdate();
			
			System.out.println("data_link 1로 변경");

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
		
		
		// uploadFile.UploadgFileInfo 에도 연계 데이터 테이블 이름 저장
		
				try {
					
					
					
					con3 = JdbcUtil.getConnection();

					System.out.println("(2)uploadFile DB connect success");

					String sql = "UPDATE uploadFile.UploadFileInfo set link_data_tablename = '"+nhisID+"_"+nhisTableName+"_"+statID+"_"+statTableName+"' where id_filename= '"+nhisTableName+ ".csv'";

					System.err.println("테이블 이름 : "+nhisTableName);
					
					pstmt3 = con3.prepareStatement(sql);
					pstmt3.executeUpdate();
					
					System.out.println("연계 데이터 테이블 이름 : "+nhisID+"_"+nhisTableName+"_"+statID+"_"+statTableName+" where id_filename= '"+nhisTableName);

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
		
		
		
		RequestDispatcher rd = request.getRequestDispatcher("/J_LINK/data_link_result.jsp");
		rd.forward(request, response);
		
		
		
	}

}
