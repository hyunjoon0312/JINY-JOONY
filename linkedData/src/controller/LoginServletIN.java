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

import svc.LoginServiceIN;
import vo.MemberIN;

/**
 * Servlet implementation class LoginServlet
 */
@WebServlet("/J_IN/loginIN")
public class LoginServletIN extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public LoginServletIN() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		
		request.setCharacterEncoding("utf-8");
		
		String INid = request.getParameter("INid");
		String INpasswd = request.getParameter("INpasswd");
		LoginServiceIN loginServiceIN = new LoginServiceIN();
		MemberIN loginMemberIN = loginServiceIN.getLoginMember(INid,INpasswd);
		//로그인이 성공되면 Member 객체가 넘어오고 실패하면 null이 넘어옴
		
		if(loginMemberIN != null){
			HttpSession session = request.getSession();
			session.setAttribute("INid", loginMemberIN);
//			response.sendRedirect("AfterLoginIN.jsp");
			RequestDispatcher rd = request.getRequestDispatcher("./LoginStatusIN.jsp");
			rd.forward(request, response);
			System.out.println("로그인 성공");
		}else{
			response.setCharacterEncoding("UTF-8");
			
			response.setContentType("text/html; charset=UTF-8");
			PrintWriter out = response.getWriter();
			out.println("<script>");
			out.println("alert('로그인 실패');");
			out.println("location.replace('./LoginFormIN.jsp')");
			out.println("</script>");
			out.close();
			System.out.println("로그인 실패");
			
		}
	}

}
