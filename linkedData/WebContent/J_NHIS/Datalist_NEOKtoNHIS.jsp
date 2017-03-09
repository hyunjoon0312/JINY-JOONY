<%@page import="vo.MemberNHIS"%>
<%@page import="db.JdbcUtil"%>
<%@page import="java.util.Date"%>
<%@page import="java.sql.Time"%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="java.sql.Timestamp"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.Statement"%>
<%@page import="java.sql.Connection"%>


<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>데이터 목록</title>
</head>
<body>


	<%
	String NHISid = ((MemberNHIS)request.getSession().getAttribute("NHISid")).getNHISId();
	String NHISname = ((MemberNHIS)request.getSession().getAttribute("NHISid")).getNHISName();
	
	
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");	
	
		Connection conn = JdbcUtil.getConnection();
		Statement stmt = null;
		ResultSet rs = null;

		try {
			stmt = conn.createStatement();

			String query = "select * from nhis_take_data.nhis_take_data_info";
			rs = stmt.executeQuery(query);
	%>


<h1 align="center">건강보험공단 데이터 목록</h1><br>
		<table id = "tb" border="1" cellspacing="0" style="margin:0 auto; text-align :center;">
<tr>
<td>


						<table border="1" cellspacing="0">
		
			<tr>
				<th>요청 시간</th>
				<th>NECA요청자 ID</th>
				<th>NECA요청자 이름</th>
				<th>요청 데이터 수</th>
				<th>전송 가능 데이터 수</th>
				<th>NECA INDEXER</th>
				<th>연계 플랫폼
			</tr>
			<%
				while (rs.next()) { //rs 를 통해 테이블 객체들의 필드값을 넘겨볼 수 있다.
					String time = rs.getString(1);
					Date date_time = format.parse(time);
					String format_time = (String)format.format(date_time);
					String neokID = rs.getString(3);
					String neokName = rs.getString(4);
					String tableName = rs.getString(2);
					
					int send_indexer = rs.getInt(7);
					int receive_indexer = rs.getInt(8);
					int send_link = rs.getInt(9);
			%>

			<tr>
				<td><%=format_time %></td>
				<td><%=neokID%></td>
				<td><%=neokName%></td>
				<td><%=rs.getInt(5) %></td>
				<td><%=rs.getInt(6)%></td>
				
				<form action="datasend_NHIStoIN" method="POST">
				<input type="hidden" name="tableName" value="<%=tableName %>"/>
				<input type="hidden" name="neokID" value="<%=neokID %>"/>
				<input type="hidden" name="NHISid" value="<%=NHISid %>"/>
				<input type="hidden" name="NHISname" value="<%=NHISname %>"/>
				<input type="hidden" name="send_indexer" value="<%=send_indexer %>"/>

				<%if(send_indexer == 0){ %>
				<td><input type="submit" value="전송"/></td>
				<%}else{ %>
				<td><input type="submit" value="전송완료" disabled="disabled"/></td>
				<%} %>				
				</form>
				
				
		
				<form action="datasend_NHIStoLINK" method="POST">
				<input type="hidden" name="tableName" value="<%=tableName %>"/>
				<input type="hidden" name="NHISid" value="<%=NHISid %>"/>
				
				<%if(receive_indexer == 0 && send_link == 0){ %>
				<td><input type = "submit" value ="연결번호 받지않음" disabled="disabled"/></td>
				<%}else if(receive_indexer == 1 && send_link == 0){ %>
				<td><input type = "submit" value = "데이터 전송" /></td>
				<%}else{ %>
				<td><input type="submit" value = "전송완료" disabled="disabled"/></td>
				<%} %>
				</form>
				
			<%
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