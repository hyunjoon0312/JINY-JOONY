<%@page import="java.util.Date"%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="db.JdbcUtil"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.Statement"%>
<%@page import="java.sql.Connection"%>
<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>통계청 데이터 보기</title>
</head>
<body>



<%

		System.err.println("/J_LINK/Data_stat.jsp");


		String statID = request.getParameter("data_statID");
		String tableName = request.getParameter("statTableName");
		String str_requestTime = request.getParameter("requestTime");
		
		
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date_time = format.parse(str_requestTime);
		String requestTime = (String)format.format(date_time);

		
		
		
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		
		
		try {
			con = JdbcUtil.getConnection();
			
			stmt = con.createStatement();

			String query = "select * from link_take_stat."+statID+"_"+tableName;
			rs = stmt.executeQuery(query);
			
			System.out.println("(1)stmt");

	%>
	
	<h2 align="center">데이터 전송 시간 : <%=requestTime %></h2>
	<h2 align="center">데이터 전송자 ID: <%=statID %></h2><br>
	
		<table id = "tb" border="1" cellspacing="0" style="margin:0 auto; text-align :center;">
	<tr>
	<td>
	
	<table border="0">
		<tr>

			<!-- ---------- link가 받은 stat 데이터 ----------- -->
			<td>

				<table border="1" cellspacing="0">
					<tr>
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
						<td><%=rs.getString(2)%></td>
						<td><%=rs.getInt(3)%></td>
						<td><%=rs.getInt(4)%></td>
						<td><%=rs.getString(5)%></td>
						<td><%=rs.getInt(6)%></td>
						<td><%=rs.getInt(7)%></td>
						<td><%=rs.getString(8)%></td>
						<td><%=rs.getInt(9)%></td>
						<td><%=rs.getInt(10)%></td>
						<td><%=rs.getString(11)%></td>
						<td><%=rs.getString(12)%></td>
						<td><%=rs.getInt(13)%></td>
						



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
</td>
</tr>
</table>
	<br><center><input type="button" name="back" value="뒤로가기" onclick="javascript:location.href='./Datalist_INDEXERtoLINK.jsp';"/></center>

</body>
</html>