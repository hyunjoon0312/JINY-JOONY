<%@page import="vo.MemberNEOK"%>
<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>

<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>NECA 승인처 로그인</title>

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
	MemberNEOK NEOKid = ((MemberNEOK)session.getAttribute("NEOKid"));
	//String seNEOKid = (String)session.getAttribute("NEOKid");

	if(NEOKid == null){
%>
    <section  id = "loginFormArea">
	<h1>보건의료 플랫폼(NECA 승인처)</h1>
	<form action="loginNEOK"  method = "POST">
		<fieldset>
			<table>
				<tr>
					<td class = "td_left">
					<label for = "id">아이디 : </label>
					</td>
					<td class = "td_right">
					<input type = "text" name = "NEOKid" id = "NEOKid"/>
					</td>
				</tr>
				<tr>
					<td class = "td_left">
					<label for = "passwd">비밀번호 : </label>
					</td>
					<td class = "td_right">
					<input type = "password" name = "NEOKpasswd" id = "NEOKpasswd"/>
					</td>
				</tr>
			</table>
			
			
			
			<!-- <button id="login-button" class="btn btn-default" type="submit">LOGIN</button> -->
			<input type = "submit" value = "로그인" id = "selectButton" />
			
			
		</fieldset>
	</form>
	</section>
<%
	}else{
		response.sendRedirect("./AfterLoginNEOK.jsp");}
%>


</body>


</html>