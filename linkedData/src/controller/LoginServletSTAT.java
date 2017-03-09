package controller;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import svc.LoginServiceSTAT;
import vo.MemberSTAT;

/**
 * Servlet implementation class LoginServlet
 */
@WebServlet("/J_STAT/loginSTAT")
public class LoginServletSTAT extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public LoginServletSTAT() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		
		System.err.println("LoginServletSTAT");
		
		request.setCharacterEncoding("utf-8");
		
		String STATid = request.getParameter("STATid");
		String STATpasswd = request.getParameter("STATpasswd");
		LoginServiceSTAT loginServiceSTAT = new LoginServiceSTAT();
		MemberSTAT loginMemberSTAT = loginServiceSTAT.getLoginMember(STATid,STATpasswd);
		//로그인이 성공되면 Member 객체가 넘어오고 실패하면 null이 넘어옴
		
		if(loginMemberSTAT != null){
			HttpSession session = request.getSession();
			session.setAttribute("STATid",  loginMemberSTAT);
//			response.sendRedirect("/J_STAT/LoginStatusSTAT.jsp");
			RequestDispatcher rd = request.getRequestDispatcher("./LoginStatusSTAT.jsp");
			rd.forward(request, response);
			System.out.println("로그인성공");
		}else{
			response.setCharacterEncoding("UTF-8");
			
			response.setContentType("text/html; charset=UTF-8");
			PrintWriter out = response.getWriter();
			out.println("<script>");
			out.println("alert('로그인 실패');");
			out.println("location.replace('./LoginFormSTAT.jsp')");
			out.println("</script>");
			out.close();
			System.out.println("로그인 실패");
			
		}
	}

}
