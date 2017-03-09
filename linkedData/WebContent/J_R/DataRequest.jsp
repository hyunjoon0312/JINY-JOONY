<%@ page language="java" contentType="text/html; charset=UTF-8"%>
<%@ page import="com.oreilly.servlet.MultipartRequest"%>
<%@ page import="com.oreilly.servlet.multipart.DefaultFileRenamePolicy"%>
<%@ page import="java.io.File" %>  
<%@ page import="java.util.*"%>
<%@ page import = "java.sql.*" %>
<%@ page import = "svc.CSVRead" %> 

<%!
CSVRead csvRead = new CSVRead();
ArrayList<String> readList = new ArrayList<String>();
%>

<%

//------------------------------- 파일 업로드 --------------------------------------
	//업로드 파일이 저장되는 실제 물리적인 경로
	String uploadPath = request.getRealPath("/upload");
	
	System.out.println(uploadPath);
	
	int size = 1024 * 1024 * 1024; // 업로드파일 최대용량 1G
	String id = "";
	String name = "";
	String subject = "";
	String filename = "";
	String origfilename = "";
	int filesize=0;
	String filepath="";
	String nhis = "";
	String stat = "";

	

	try {
		//request 객체가 가지고 있는 값을 변화해서 접근해야 한다.
		MultipartRequest multi = new MultipartRequest(request, uploadPath, size, "UTF-8",
				new DefaultFileRenamePolicy());
		
		//multi객체에서 사용자가 입력 요청한 값 가져오기
		id = multi.getParameter("id");
		name = multi.getParameter("name");
		subject = multi.getParameter("subject");
		
		// checkbox에서 데이터 받아옴
		nhis = multi.getParameter("nhis") == null ? "0" : multi.getParameter("nhis");;
		stat = multi.getParameter("stat") == null ? "0" : multi.getParameter("stat");;
		
		// 업로드 성공한 파일 상세정보
		
		//열거형 Enumeration
		Enumeration files = multi.getFileNames();
		
		//첫번째 파일입력상자의 이름을 가져온다.
		String file = (String) files.nextElement();	
		
		//multi 객체에 있는 file의 실제 파일명을 가져온다.
		filename = multi.getFilesystemName(file);	//서버에 실제 저장된 파일명
		origfilename = multi.getOriginalFileName(file);	//업로드시 파일의 파일명

		
		//file 정보 가져오기
		File fileinfo = multi.getFile(file);
		filesize = (int)fileinfo.length();	// 파일 크기
		filepath = (String)fileinfo.getPath();	//파일 경로 + 파일명
		
		

		
	} catch (Exception e) {
		e.printStackTrace();
	}
	
	
	//-----------------------------------------------------파일 정보 DB에 저장 ----------------------------------------------------------------
	
	Connection con = null;                                        // null로 초기화 한다.
	PreparedStatement pstmt1 = null;
	PreparedStatement pstmt2 = null;
	PreparedStatement pstmt3 = null;

	try{
	
		Class.forName("com.mysql.jdbc.Driver");
		con = DriverManager.getConnection("jdbc:mysql://112.72.158.187:3306/uploadFile", "hyunjoon",
				"hyunjoon");
		System.out.println("UploadDB connect success");
		

		
		
	int IntNhis;
	int IntStat;
	

	//checkbox에서 받아온 데이터 int형으로 캐스팅
	IntNhis = Integer.parseInt(nhis);
	IntStat = Integer.parseInt(stat);

	String infosql = "insert into UploadFileInfo values(default,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";        // sql 쿼리
	pstmt1 = con.prepareStatement(infosql);                          // prepareStatement에서 해당 sql을 미리 컴파일한다.
	pstmt1.setString(1,subject);
	pstmt1.setString(2,filename);
	pstmt1.setString(3,filepath);
	pstmt1.setInt(4,filesize);
	pstmt1.setString(5,id);
	pstmt1.setString(6,name);
	pstmt1.setInt(7, IntNhis);
	pstmt1.setInt(8, IntStat);
	pstmt1.setInt(9, 0);
	pstmt1.setInt(10, 0);
	pstmt1.setInt(11, 0);
	pstmt1.setInt(12, 0);
	pstmt1.setString(13, "");
	pstmt1.setString(14, id+"_"+filename);
	
	//pstmt.setTimestamp(4, new Timestamp(System.currentTimeMillis()));    // 현재 날짜와 시간

	pstmt1.executeUpdate();                                        // 쿼리를 실행한다.

	System.out.println("upload 테이블에 새로운 파일정보를 추가했습니다.");        // 성공시 메시지 출력

	
	
	
	//------------------------------------ 데이터 저장할 테이블 생성 ----------------------------------------------------
	
	String sqlFilename = filename;
	sqlFilename = sqlFilename.replace(".csv", "");
	
	
	//나중에 내 방식대로 구현할때 사용할 sql문
	//String cTableSql = "create table uploadFile."+id+"_"+sqlFilename+"(linkID int(20) NOT NULL AUTO_INCREMENT, personID varchar(255), primary key(linkID))";
	String cTableSql = "create table uploadFile."+id+"_"+sqlFilename+"(personID varchar(255), primary key(personID))";
	pstmt2 = con.prepareStatement(cTableSql);                          // prepareStatement에서 해당 sql을 미리 컴파일한다.

	
	pstmt2.executeUpdate();

	System.out.println("테이블을 생성하였습니다");

	
	
	//-------------------------------- 위에서 생성된 테이블에 데이터 저장 + 연결번호 --------------------------------------------
	
	
	readList = csvRead.readCSV();
	
	for(int i=0 ; i<readList.size(); i++){
		
		//나중에 내 방식대로 구현할때 연결번호를 위한 쿼리
		//String inputSql = "INSERT INTO uploadFile."+name+"_"+sqlFilename+" Values(default, ?)";	
		
		
		String inputSql = "INSERT INTO uploadFile."+id+"_"+sqlFilename+" Values(?)";	
		//String inputSql = "LOAD DATA LOCAL INFILE ? INTO TABLE uploadFile."+name+"_"+sqlFilename+" FIELDS TERMINATED BY ','";
		pstmt3 = con.prepareStatement(inputSql);
		pstmt3.setString(1, readList.get(i));
		pstmt3.executeUpdate();
	}
	
	
	
	System.out.println(filepath+"파일 입력완료");
	
	}catch(Exception e){                                                    // 예외가 발생하면 예외 상황을 처리한다.
	e.printStackTrace();
	System.out.println("upload 테이블에 새로운 파일정보 추가에 실패했습니다.");
	}finally{                                                            // 쿼리가 성공 또는 실패에 상관없이 사용한 자원을 해제 한다. (순서중요)
	if(pstmt1 != null) try{pstmt1.close();}catch(SQLException sqle){}            // PreparedStatement 객체 해제
	if(pstmt2 != null) try{pstmt2.close();}catch(SQLException sqle){}            // PreparedStatement 객체 해제
	if(pstmt3 != null) try{pstmt3.close();}catch(SQLException sqle){}            // PreparedStatement 객체 해제
	if(con != null) try{con.close();}catch(SQLException sqle){}            // Connection 해제
	}
	
	
	
	
%>
<html>
<body>
	<form name="filecheck" action="fileCheck.jsp" method="post">
		<input type="hidden" name="id" value="<%=id%>"> 
		<input type="hidden" name="name" value="<%=name%>"> 
		<input type="hidden" name="subject" value="<%=subject%>"> 
		<input type="hidden" name="filename" value="<%=filename%>"> 
		<input type="hidden" name="origfilename" value="<%=origfilename%>">
	</form>
	<a href="#" onclick="javascript:filecheck.submit()"><h2>업로드 확인 및 다운로드
		페이지 이동</h2></a>
		
		<%-- <% 	String tom_path = System.getProperty("catalina.base");
			out.println("tomcat 위치 : " +tom_path);
		%> --%>
</body>

   <script type="text/javascript">  
    var ws = new WebSocket("ws://localhost:8080/linkedData/websocket");
	
       if ("WebSocket" in window)
       {
          // Let us open a web socket
      
          ws.onopen = function()
          {
             // Web Socket is connected, send data using send()
             ws.send("<%=name%>님(연구자) : <%=origfilename%> 파일 업로드"+"\n");
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
 
    </script>

</html>
