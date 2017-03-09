<%@page import="vo.MemberNHIS"%>
<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>

<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>건보공단 로그인</title>
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
	MemberNHIS NHISid = ((MemberNHIS)session.getAttribute("NHISid"));
	//String seNHISid = (String)session.getAttribute("NHISid");


	if(NHISid == null){
%>
    <section  id = "loginFormArea">
	<h1>보건의료 플랫폼(건보)</h1>
	<form action="loginNHIS" method = "POST">
		<fieldset>
			<table>
				<tr>
					<td class = "td_left">
					<label for = "id">아이디 : </label>
					</td>
					<td class = "td_right">
					<input type = "text" name = "NHISid" id = "NHISid"/>
					</td>
				</tr>
				<tr>
					<td class = "td_left">
					<label for = "passwd">비밀번호 : </label>
					</td>
					<td class = "td_right">
					<input type = "password" name = "NHISpasswd" id = "NHISpasswd"/>
					</td>
				</tr>
			</table>
			
			<input type = "submit" value = "로그인" id = "selectButton"/>
		</fieldset>
	</form>
	</section>
<%
	}else{
		response.sendRedirect("AfterLoginNHIS.jsp");}
%>
</body>
</html>