<%@page import="vo.MemberNEOK"%>
<%@page import="java.sql.Statement"%>
<%@page import="db.JdbcUtilUpload"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.PreparedStatement"%>
<%@page import="java.sql.Connection"%>
<%@page import="java.sql.DriverManager"%>
<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>통계청 데이터 요청 결과</title>
</head>
<body>

<%

String NEOKid = ((MemberNEOK)request.getSession().getAttribute("NEOKid")).getNEOKId();

String filename = request.getParameter("filename");
String uploadername = request.getParameter("uploadername");
String str_stat_send = request.getParameter("stat_send");
int stat_send = Integer.parseInt(str_stat_send);

if(stat_send == 0){
%>

<h1>통계청으로 <%=uploadername %> 연구자가 요청한 데이터 전송 성공<br/></h1>

<input type="button" name="back" value="뒤로가기" onclick="javascript:location.href='./Datalist_RtoNEOK.jsp';"/>


  <script type="text/javascript">  
    var ws = new WebSocket("ws://localhost:8080/linkedData/websocket");
	
       if ("WebSocket" in window)
       {
          // Let us open a web socket
      
          ws.onopen = function()
          {
             // Web Socket is connected, send data using send()
             ws.send("NECA승인처(<%=NEOKid%>) : <%=uploadername%>연구자 요청한 식별자 통계청 전송"+"\n");
          };
			
          ws.onmessage = function (evt) 
          { 
             //var received_msg = evt.data;
             
          };
			
          ws.onclose = function()
          { 
             // websocket is closed.
             alert("Connection is closed..."); 
          };
       }
       
       else
       {
          // The browser doesn't support WebSocket
          alert("WebSocket NOT supported by your Browser!");
       }
    function send(){
    	console.log(123)
    	ws.send("123");
    }
 
    </script>


<%}else{ %>

<h1>이미 전송한 데이터 입니다.<br></h1>

<input type="button" name="back" value="뒤로가기" onclick="javascript:location.href='./Datalist_RtoNEOK.jsp';"/>


<%} %>




    
    	
    
</body>
</html>