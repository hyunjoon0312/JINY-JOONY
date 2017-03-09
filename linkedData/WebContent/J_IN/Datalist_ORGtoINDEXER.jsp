<%@page import="vo.MemberIN"%>
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
<title>데이터목록</title>
</head>
<body>
	<%
		String INid = ((MemberIN) request.getSession().getAttribute("INid")).getINId();
		String INname = ((MemberIN) request.getSession().getAttribute("INid")).getINName();

		Connection conn1 = JdbcUtil.getConnection();
		Statement stmt1 = null;
		ResultSet rs1 = null;

		try {
			stmt1 = conn1.createStatement();

			String query = "INSERT INTO indexer_linknum.indexer_take_data_info_secret(nhisRequestTime, nhisID, nhisName, nhisTableName, statRequestTime, statID, statName, statTableName) SELECT indexer_take_nhis.indexer_take_nhis_info.requestTime, indexer_take_nhis.indexer_take_nhis_info.nhisID, indexer_take_nhis.indexer_take_nhis_info.nhisName, indexer_take_nhis.indexer_take_nhis_info.table_name, indexer_take_stat.indexer_take_stat_info.requestTime, indexer_take_stat.indexer_take_stat_info.statID, indexer_take_stat.indexer_take_stat_info.statName, indexer_take_stat.indexer_take_stat_info.table_name FROM indexer_take_nhis.indexer_take_nhis_info INNER JOIN indexer_take_stat.indexer_take_stat_info ON indexer_take_nhis.indexer_take_nhis_info.table_name = indexer_take_stat.indexer_take_stat_info.table_name WHERE indexer_take_nhis.indexer_take_nhis_info.join_result = 0 and indexer_take_stat.indexer_take_stat_info.join_result = 0 ";
			stmt1.executeUpdate(query);

			System.out.println("(1)stmt1");
			
		} catch (Exception e) {
			out.println("err:" + e.toString());
		} finally {
			if (rs1 != null) {
				rs1.close();
			} // ResultSet exit
			if (stmt1 != null) {
				stmt1.close();
			} // Statement exit
			if (conn1 != null) {
				conn1.close();
			} // Connection exit
		}

		Connection conn2 = JdbcUtil.getConnection();
		Statement stmt2 = null;
		ResultSet rs2 = null;

		try {
			stmt2 = conn2.createStatement();

			String query = "UPDATE indexer_take_nhis.indexer_take_nhis_info set join_result = 1 where indexer_take_nhis.indexer_take_nhis_info.join_result = 0";
			stmt2.executeUpdate(query);
			
			System.out.println("(2)stmt2");

			
		} catch (Exception e) {
			out.println("err:" + e.toString());
		} finally {
			if (rs2 != null) {
				rs2.close();
			} // ResultSet exit
			if (stmt2 != null) {
				stmt2.close();
			} // Statement exit
			if (conn2 != null) {
				conn2.close();
			} // Connection exit
		}

		
		
		
		Connection conn3 = JdbcUtil.getConnection();
		Statement stmt3 = null;
		ResultSet rs3 = null;

		try {
			stmt3 = conn3.createStatement();

			String query = "UPDATE indexer_take_stat.indexer_take_stat_info set join_result = 1 where indexer_take_stat.indexer_take_stat_info.join_result = 0";
			stmt3.executeUpdate(query);
			
			System.out.println("(3)stmt3");

			
		} catch (Exception e) {
			out.println("err:" + e.toString());
		} finally {
			if (rs3 != null) {
				rs3.close();
			} // ResultSet exit
			if (stmt3 != null) {
				stmt3.close();
			} // Statement exit
			if (conn3 != null) {
				conn3.close();
			} // Connection exit
		}
		
		Connection conn4 = JdbcUtil.getConnection();
		Statement stmt4 = null;
		ResultSet rs4 = null;
		
		
		try {
			stmt4 = conn4.createStatement();

			String query = "select * from indexer_linknum.indexer_take_data_info_secret";
			rs4 = stmt4.executeQuery(query);
			
			System.out.println("(4)stmt4");

	%>
<h1 align="center">NECA INDEXER 데이터 목록</h1><br>
			<table id = "tb" border="1" cellspacing="0" style="margin:0 auto; text-align :center;">
	
		<tr>

			<!-- ---------- indexer가 받은 데이터 info ----------- -->
			<td>


			<table border="1">
		
		
					<tr>
						<th>순서</th>
						<th>NHIS 요청 시간</th>
						<th>NHIS 요청자 ID</th>
						<th>NHIS 요청자 이름</th>
						<th>STAT 요청 시간</th>
						<th>STAT 요청자 ID</th>
						<th>STAT 요청자 이름</th>
						<th>암호키 생성</th>
						<th>기관 전송</th>
						<th>암호키 전송</th>
					</tr>
					<%
						while (rs4.next()) { //rs 를 통해 테이블 객체들의 필드값을 넘겨볼 수 있다.
								String seqID = rs4.getString(1);
								String nhisRequestTime = rs4.getString(2);
								String nhisID = rs4.getString(3);
								String nhisName = rs4.getString(4);
								String nhisTableName = rs4.getString(5);
								String statRequestTime = rs4.getString(6);
								String statID = rs4.getString(7);
								String statName = rs4.getString(8);
								String statTableName = rs4.getString(9);
								
								
								int org_send = rs4.getInt(10);
								String secretkey = rs4.getString(11);
								int secretkey_send = rs4.getInt(12);		
								int create_secretkey = rs4.getInt(13);
					%>

					<tr>
						<td><%=seqID%></td>
						<td><%=nhisRequestTime%></td>
						<td><%=nhisID%></td>
						<td><%=nhisName%></td>
						<td><%=statRequestTime%></td>
						<td><%=statID%></td>
						<td><%=statName%></td>

						
						<form action="create_secretkey" method="POST">
						
							<input type="hidden" name="nhisTableName" value="<%=nhisTableName%>" />
							<input type="hidden" name="statTableName" value="<%=statTableName%>" />	
							<input type="hidden" name="nhisID" value="<%=nhisID%>" />
							<input type="hidden" name="statID" value="<%=statID%>" />
								
							<%if(create_secretkey == 0){ %>
							<td><input type="submit" value="암호키 생성"/></td>
							<%}else{ %>
							<td><input type="submit" value="생성완료"/ disabled="disabled"></td>				
							<%} %>
						</form>





						<form action="datasend_INtoORG" method="POST">
							<input type="hidden" name="nhisTableName" value="<%=nhisTableName%>" />
							<input type="hidden" name="statTableName" value="<%=statTableName%>" />
							<input type="hidden" name="org_send" value="<%=org_send%>" />
							<input type="hidden" name="nhisID" value="<%=nhisID%>" />
							<input type="hidden" name="statID" value="<%=statID%>" />
							<input type="hidden" name="secretkey" value="<%=secretkey %>" />

							<%
								if (org_send == 0 && secretkey_send == 1) {
							%>
							<td><input type="submit" value="전송" /></td>
							<%
								} else if(org_send == 1 && secretkey_send == 1) {
							%>
							<td><input type="submit" value="전송완료" disabled="disabled" /></td>
							<%
								}else if(org_send == 0 && secretkey_send == 0){
							%>
							<td><input type="submit" value="암호키 미생성" disabled="disabled" /></td>
							<%} %>
						</form>





						<form action="datasend_INtoLINK" method="POST">
							<input type="hidden" name="nhisTableName" value="<%=nhisTableName%>" />
							<input type="hidden" name="statTableName" value="<%=statTableName%>" />
							<input type="hidden" name="statID" value="<%=statID%>" />
							<input type="hidden" name="nhisID" value="<%=nhisID%>" />
							<input type="hidden" name="inID" value="<%=INid%>" />
							<input type="hidden" name="inName" value="<%=INname%>" />
							<input type="hidden" name="secretkey" value="<%=secretkey %>" />
							<input type="hidden" name="secretkey_send" value="<%=secretkey_send %>" />
							
							
							


							<%
								if (secretkey_send == 0 && create_secretkey == 0) {
							%>
							<td><input type="submit" value="암호키 미생성" disabled="disabled"/></td>
							<%
								}else if (secretkey_send == 0 && create_secretkey == 1) {
							%>
							<td><input type="submit" value="전송"  /></td>
							<%
								}else{
							%>
							<td><input type="submit" value="전송완료" disabled="disabled" /></td>
							<%} %>
						</form>
						

						<%
							} // end while
						%>


					</tr>
				</table> <%
 	} catch (Exception e) {
 		out.println("err:" + e.toString());
 	} finally {
 		if (rs4 != null) {
 			rs4.close();
 		} // ResultSet exit
 		if (stmt4 != null) {
 			stmt4.close();
 		} // Statement exit
 		if (conn4 != null) {
 			conn4.close();
 		} // Connection exit
 	}
 %>


			</td>
		</tr>
	</table>

</body>
</html>