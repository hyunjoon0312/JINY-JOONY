<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<%@page import="dao.LoginDAONEOK"%>
<%@page import='vo.MemberNEOK' %>    
    
<%
	String NEOKid = ((MemberNEOK)request.getSession().getAttribute("NEOKid")).getNEOKId();
	String NEOKname = ((MemberNEOK)request.getSession().getAttribute("NEOKid")).getNEOKName();
	
%>
    
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>NECA 승인처 로그인</title>
</head>
<body>

<h1>NECA 승인처 로그인 성공<br></h1><br>
      <script type="text/javascript">  
    var ws = new WebSocket("ws://localhost:8080/linkedData/websocket");
	
       if ("WebSocket" in window)
       {
          // Let us open a web socket
      
          ws.onopen = function()
          {
             // Web Socket is connected, send data using send()
             ws.send("<%=NEOKname%>님(NECA승인처) 로그인"+"\n");
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
 
<ul>
<li><h2><a href = "./Datalist_RtoNEOK.jsp">연계 데이터 요청 목록</a></h2></li>
</ul>


	<br>&nbsp; &nbsp;&nbsp; &nbsp;&nbsp; &nbsp;&nbsp; &nbsp;&nbsp; &nbsp;
	<input type="button" name="back" value="로그아웃"
		onclick="javascript:location.href='./sessionLogoutNEOK.jsp';" />


</body>
</html>