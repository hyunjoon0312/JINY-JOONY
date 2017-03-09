<%@page import="db.JdbcUtil"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.Statement"%>
<%@page import="java.sql.Connection"%>
<%@ page language="java" contentType="text/html; charset=EUC-KR"
    pageEncoding="EUC-KR"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=EUC-KR">
<title>연계데이터 확인</title>
</head>
<body>

<%
		System.err.println("/J_R/link_data_view.jsp");

	

		String link_data_tablename = request.getParameter("link_data_tablename");
		
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		
		
		
		
		try {
			
			con = JdbcUtil.getConnection();
			
			stmt = con.createStatement();

			String query = "select * from link_safe_DB."+link_data_tablename;
			rs = stmt.executeQuery(query);
			
			System.out.println("(1)stmt");

			
			
	%>
	
	
	<table id = "tb" border="1" cellspacing="0" style="margin:0 auto; text-align :center;">
	
	
	
		<tr>

			<td>

				<table border="1" cellspacing="0">
					<tr>
						<th>번호</th>
						<th>STND_Y</th>
						<th>YKIHO_ID</th>
						<th>YKIHO_GUBUN_CD</th>
						<th>ORG_TYPE</th>
						<th>YKIHO_SIDO</th>
						<th>SICKBED_CNT</th>
						<th>DR_CNT</th>
						<th>CY_YN</th>
						<th>MRI_YN</th>
						<th>PER_YN</th>
						<th>REPORT_YMD</th>
						<th>ADDRESS</th>
						<th>GENDER</th>
						<th>DEATH_YMD</th>
						<th>DEATH_TIME</th>
						<th>DEATH_PLACE</th>
						<th>DEATH_JOB</th>
						<th>MARRY</th>
						<th>EDU</th>
						<th>DEATH_CAU1</th>
						<th>DEATH_CAU1_Parent</th>
						<th>DEATH_AGE</th>
					</tr>
					<%
						while (rs.next()) { //rs 를 통해 테이블 객체들의 필드값을 넘겨볼 수 있다.
								
								
							
							
					%>

					<tr>
						<td><%=rs.getInt(1) %></td>
						<td><%=rs.getInt(2) %></td>
						<td><%=rs.getInt(3) %></td>
						<td><%=rs.getInt(4) %></td>
						<td><%=rs.getInt(5) %></td>
						<td><%=rs.getInt(6) %></td>
						<td><%=rs.getInt(7) %></td>
						<td><%=rs.getInt(8) %></td>
						<td><%=rs.getInt(9) %></td>
						<td><%=rs.getInt(10) %></td>
						<td><%=rs.getInt(11) %></td>
						<td><%=rs.getString(12)%></td>
						<td><%=rs.getInt(13)%></td>
						<td><%=rs.getInt(14)%></td>
						<td><%=rs.getString(15)%></td>
						<td><%=rs.getInt(16)%></td>
						<td><%=rs.getInt(17)%></td>
						<td><%=rs.getString(18)%></td>
						<td><%=rs.getInt(19)%></td>
						<td><%=rs.getInt(20)%></td>
						<td><%=rs.getString(21)%></td>
						<td><%=rs.getString(22)%></td>
						<td><%=rs.getInt(23)%></td>
					




						<%
							} // end while
						%>


					</tr>
				</table> <%
 	} catch (Exception e) {
 		out.println("err:" + e.toString());
 	} finally {
 		if (rs != null) {
 			rs.close();
 		} // ResultSet exit
 		if (stmt != null) {
 			stmt.close();
 		} // Statement exit
 		if (con != null) {
 			con.close();
 		} // Connection exit
 	}
		
%>
</td>
</tr>
</table>
	<br><center><input type="button" name="back" value="뒤로가기" onclick="javascript:location.href='./Request_list.jsp';"/></center>




</body>
</html>