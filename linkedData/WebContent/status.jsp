<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>상태 확인창</title>

</head>

<style type="text/css">

#center {
width:50px; height:50px;  display:inline-block; }


</style>
<body>
<h2>상태확인창</h2>

    <br />
    
    <textarea id="messageTextArea" rows="30" cols="80" ></textarea>
     
    <script type="text/javascript">  
    var ws = new WebSocket("ws://localhost:8080/linkedData/websocket");
	
   function chage(){
	   
	   var element = document.getElementById("span1");
	   element.style.fontSize = "48pt";
	   element.style.color = "#f00";
	   element.style.fontWeight = "bold";

   }
    
 
    
       if ("WebSocket" in window)
       {
          
          
          // Let us open a web socket
      
          ws.onopen = function()
          {
			
        	  // Web Socket is connected, send data using send()
             ws.send("대기중..."+"\n");
             
          };
			
          ws.onmessage = function (evt) 
          { console.log(evt)
             //var received_msg = evt.data;
             
             messageTextArea.value=messageTextArea.value+evt.data;
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
    
    
    
    <!-- <input type="submit" value="send" onclick="send()" /> -->
</body>
</html>


