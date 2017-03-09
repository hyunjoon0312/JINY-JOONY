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

import svc.LoginServiceLINK;
import vo.MemberLINK;

/**
 * Servlet implementation class LoginServlet
 */
@WebServlet("/J_LINK/loginLINK")
public class LoginServletLINK extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public LoginServletLINK() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		
		
		request.setCharacterEncoding("utf-8");
		
		String LINKid = request.getParameter("LINKid");
		String LINKpasswd = request.getParameter("LINKpasswd");
		LoginServiceLINK loginServiceLINK = new LoginServiceLINK();
		MemberLINK loginMemberLINK = loginServiceLINK.getLoginMember(LINKid,LINKpasswd);
		//로그인이 성공되면 Member 객체가 넘어오고 실패하면 null이 넘어옴
		
		if(loginMemberLINK != null){
			HttpSession session = request.getSession();
			session.setAttribute("LINKid", loginMemberLINK);
//			response.sendRedirect("/J_IN/LoginStatusLINK.jsp");
			RequestDispatcher rd = request.getRequestDispatcher("./LoginStatusLINK.jsp");
			rd.forward(request, response);
			System.out.println("로그인성공");
		}else{
			response.setCharacterEncoding("UTF-8");
			
			response.setContentType("text/html; charset=UTF-8");
			PrintWriter out = response.getWriter();
			out.println("<script>");
			out.println("alert('로그인 실패');");
			out.println("location.replace('./LoginFormLINK.jsp')");
			out.println("</script>");
			out.close();
			System.out.println("로그인 실패");
			
		}
	}

}
