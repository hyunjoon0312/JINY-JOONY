<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<%@ page import="vo.MemberLINK" %>

<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>데이터연계플랫폼 로그인</title>
<style>
	#loginFormArea{
		margin : auto;
		width : 400px;
		height : 200px;
		border : 2px double black; 
		border-radius : 10px;
		text-align: center;
		margin-top : 220px;
	}
	fieldset{
		text-align: center;
		border : none;
	}
	#selectButton{
		margin-top : 20px;
	}
	table{
	    width : 380px;
		margin : auto;
	}
	.td_left{
		width : 180px
	}
	.td_right{
		width : 200px
	}
</style>
</head>
<body>
<% 
	MemberLINK LINKid = ((MemberLINK)session.getAttribute("LINKid"));
	//String seLINKid = (String)session.getAttribute("LINKid");


	if(LINKid == null){
%>
    <section  id = "loginFormArea">
	<h1>보건의료 플랫폼(데이터연계)</h1>
	<form action="loginLINK" method = "POST">
		<fieldset>
			<table>
				<tr>
					<td class = "td_left">
					<label for = "id">아이디 : </label>
					</td>
					<td class = "td_right">
					<input type = "text" name = "LINKid" id = "LINKid"/>
					</td>
				</tr>
				<tr>
					<td class = "td_left">
					<label for = "passwd">비밀번호 : </label>
					</td>
					<td class = "td_right">
					<input type = "password" name = "LINKpasswd" id = "LINKpasswd"/>
					</td>
				</tr>
			</table>
			
			<input type = "submit" value = "로그인" id = "selectButton"/>
		</fieldset>
	</form>
	</section>
<%
	}else{
		response.sendRedirect("AfterLoginLINK.jsp");}
%>
</body>
</html>