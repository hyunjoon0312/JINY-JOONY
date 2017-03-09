<%@page import="vo.MemberSTAT"%>
<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>전송 결과</title>
</head>
<body>

<h1>연계플랫폼으로 데이터 전송 완료</h1><br><br>

<%
String STATid = ((MemberSTAT)request.getSession().getAttribute("STATid")).getSTATId();
%>



<script type="text/javascript">  
    var ws = new WebSocket("ws://localhost:8080/linkedData/websocket");
	
       if ("WebSocket" in window)
       {
          // Let us open a web socket
      
          ws.onopen = function()
          {
             // Web Socket is connected, send data using send()
             ws.send("STAT(<%=STATid%>) : 연계플랫폼으로 데이터 전송" + "\n");
			};

			ws.onmessage = function(evt) {
				//var received_msg = evt.data;

			};

			ws.onclose = function() {
				// websocket is closed.
				alert("Connection is closed...");
			};
		}

		else {
			// The browser doesn't support WebSocket
			alert("WebSocket NOT supported by your Browser!");
		}
		function send() {
			console.log(123)
			ws.send("123");
		}
	</script>
	
	<input type="button" name="back" value="뒤로가기" onclick="javascript:location.href='./Datalist_NEOKtoSTAT.jsp';"/>
</body>
</html>