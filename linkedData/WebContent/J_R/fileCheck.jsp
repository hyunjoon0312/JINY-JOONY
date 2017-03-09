<%@ page language="java" contentType="text/html; charset=UTF-8"%>
<%
	request.setCharacterEncoding("UTF-8");
	String id=request.getParameter("id");
	String name=request.getParameter("name");
	String subject=request.getParameter("subject");
	String filename=request.getParameter("filename");
	String origfilename=request.getParameter("origfilename");
%>
<html>
<head>
<title>파일 업로드 확인 및 다운로드</title>
</head>
<body>
<br>
<ul>
<li><h2>ID : <%=id %></br></h2></li>
<li><h2>연구자 : <%=name %><br></h2></li>
<li><h2>제목 : <%=subject %><br></h2></li>
<li><h2>파일명 : <a href="file_down.jsp?file_name=<%=filename %>"><%=origfilename %></a><br></h2></li>

	<input type="button" name="moreUpload" value="더 업로드 하기" onclick="javascript:location.href='./DataRequestForm.jsp';"/>
</ul>
</body>
</html>
