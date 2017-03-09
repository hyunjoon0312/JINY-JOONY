package controller;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import jdk.nashorn.internal.ir.RuntimeNode.Request;
import svc.LoginServiceR;
import vo.MemberR;

/**
 * Servlet implementation class LoginServlet
 */
@WebServlet("/J_R/loginR")
public class LoginServletR extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public LoginServletR() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	
		System.err.println("LoginServeltR.java");
		
		request.setCharacterEncoding("utf-8");
		
		String Rid = request.getParameter("Rid");
		String Rpasswd = request.getParameter("Rpasswd");
		LoginServiceR loginServiceR = new LoginServiceR();
		MemberR loginMemberR = loginServiceR.getLoginMember(Rid,Rpasswd);
		//로그인이 성공되면 Member 객체가 넘어오고 실패하면 null이 넘어옴
		
		
		
		if(loginMemberR != null){
			HttpSession session = request.getSession();
			session.setAttribute("Rid",  loginMemberR);
			System.out.println("로그인성공");
			//response.sendRedirect("AfterLoginR.jsp");
			RequestDispatcher rd = request.getRequestDispatcher("./LoginStatusR.jsp");
			rd.forward(request, response);
			
			//RequestDispatcher를 통해서 로그인 상태 알림페이지를 거치고 다시 <jsp:forward page="이동할 페이지"/>; 를 통해 로그인 후 페이지로 이동한다.
			//위 방법은 페이지가 열리지 않아 사용 불가능.. 야매로 똑같은 페이지를 두개 만들고 처음 로그인시의 페이지에는 로그인 상태 알림을 날리는 소스를 넣어놓고 
			// 그 이후에 세션이 유지될시 자동으로 이동되는 페이지에는 로그인 상태 알림을 날리는 소스를 넣지 않는다.
			//pageContext.forward("이동할페이지"); 는 무엇인지 알아보기
			
		}else{
			response.setCharacterEncoding("UTF-8");
			
			response.setContentType("text/html; charset=UTF-8");
			PrintWriter out = response.getWriter();
			out.println("<script>");
			out.println("alert('로그인 실패');");
			out.println("location.replace('./LoginFormR.jsp')");
			out.println("</script>");
			out.close();
			System.out.println("로그인 실패");
			
		}
	}

}
