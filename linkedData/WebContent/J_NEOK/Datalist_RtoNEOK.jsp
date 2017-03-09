<%@page import="vo.MemberNEOK"%>
<%@page import="java.util.Date"%>
<%@page import="java.sql.Time"%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="java.sql.Timestamp"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.Statement"%>
<%@page import="db.JdbcUtilUpload"%>
<%@page import="java.sql.Connection"%>
<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%
 
    request.setCharacterEncoding("UTF-8");
response.setContentType("text/html; charset=UTF-8");


%>



<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<!-- 	<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
	합쳐지고 최소화된 최신 CSS
	<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.2/css/bootstrap.min.css">
	
	부가적인 테마
	<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.2/css/bootstrap-theme.min.css">
	
	합쳐지고 최소화된 최신 자바스크립트
	<script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.2/js/bootstrap.min.js"></script> -->
<title>데이터 요청 목록</title>
</head>
<body>

	<%
		String NEOKid = ((MemberNEOK)request.getSession().getAttribute("NEOKid")).getNEOKId();
		String NEOKname = ((MemberNEOK)request.getSession().getAttribute("NEOKid")).getNEOKName();

	
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");	
	
		Connection conn = JdbcUtilUpload.getUploadConnection();
		Statement stmt = null;
		ResultSet rs = null;

		try {
			stmt = conn.createStatement();

			String query = "select * from UploadFileInfo";
			rs = stmt.executeQuery(query);
	%>
<h1 align="center">NECA 승인처 데이터 목록</h1><br>
		<table id = "tb" border="1" cellspacing="0" style="margin:0 auto; text-align :center;">
	<tr>
	<td>

				<table border="1" cellspacing="0">
			<tr>
				<th>요청 시간</th>
				<th>제목</th>
				<th>파일 이름 </th>
				<th>연구자 ID</th>
				<th>연구자 이름</th>
				<th>건강보험공단</th>
				<th>통계청</th>
				<th>데이터 전송 여부</th>
			</tr>
			<%
				while (rs.next()) { //rs 를 통해 테이블 객체들의 필드값을 넘겨볼 수 있다.
					String time = rs.getString(1);
					Date date_time = format.parse(time);
					String format_time = (String)format.format(date_time);
					String filename = rs.getString(3);
					String uploadername = rs.getString(7);
					String uploaderid = rs.getString(6);
					int nhis = rs.getInt(8);
					int stat = rs.getInt(9);
					int nhis_send = rs.getInt(10);
					int stat_send = rs.getInt(11);
					int send_ok = rs.getInt(12);
					System.out.println(nhis_send);
			%>

			<tr>
				<td><%=format_time %></td>
				<td><%=rs.getString(2)%></td>
				<td><%=filename%></td>
				<td><%=uploaderid%></td>
				<td><%=uploadername%></td>
				
				<form action="datasend_nhis" method="POST">
				<input type="hidden" name="filename" value="<%=filename %>"/>
				<input type="hidden" name="uploadername" value="<%=uploadername %>"/>
				<input type="hidden" name="uploaderid" value="<%=uploaderid %>"/>
				<input type="hidden" name="NEOKid" value="<%=NEOKid %>"/>
				<input type="hidden" name="NEOKname" value="<%=NEOKname %>"/>
				<input type="hidden" name="nhis_send" value="<%=nhis_send %>"/>
				<input type="hidden" name="stat_send" value="<%=stat_send %>"/>
				
				<%if(nhis == 1){ %>
				<td><input type="submit" value="식별자 전송"/></td>
				<%}else{ %>
				<td><input type="submit" value="연계 미신청" disabled="disabled"/></td><%} %>
				</form>
				
				<form action="datasend_stat" method="POST">
				<input type="hidden" name="filename" value="<%=filename %>"/>
				<input type="hidden" name="uploadername" value="<%=uploadername %>"/>
				<input type="hidden" name="uploaderid" value="<%=uploaderid %>"/>
				<input type="hidden" name="NEOKid" value="<%=NEOKid %>"/>
				<input type="hidden" name="NEOKname" value="<%=NEOKname %>"/>
				<input type="hidden" name="nhis_send" value="<%=nhis_send %>"/>
				<input type="hidden" name="stat_send" value="<%=stat_send %>"/>
				
				<%if(stat == 1){ %>
				<td><input type="submit" value="식별자 전송"/></td>
				<%}else{ %>
				<td><input type="submit" value="연계 미신청" disabled="disabled"/></td><%} %>
				</form>
			<%
				if(send_ok == 1){
			%>
			<td>모든 기관 전송 완료</td>
			<%} else if(nhis_send == 1 && stat_send == 0){%>
			<td>통계청 전송 필요</td>
			<%}else if(nhis_send == 0 && stat_send == 1){ 
			%>
			<td>건강보험공단 전송 필요</td>
			<%}else{ %>
			<td>모든 기관 전송 필요</td>
			<%}
				} // end while
			%>
			
			
			</tr>
		</table>
		</td>
		</tr>
		</table>
	<%
		
		} catch (Exception e) {
			out.println("err:" + e.toString());
		}finally{
			if(rs != null){rs.close();} // ResultSet exit
			if(stmt != null){stmt.close();} // Statement exit
			if(conn != null){conn.close();} // Connection exit
		}
	%>

</body>
</html>