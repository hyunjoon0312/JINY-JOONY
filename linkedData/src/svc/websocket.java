package svc;


import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
  
@ServerEndpoint("/websocket")
public class websocket {
	
    private static Set<Session> userSessions = Collections.newSetFromMap(new ConcurrentHashMap<Session, Boolean>());

    @OnOpen
    public void onOpen(Session userSession) {
        userSessions.add(userSession);
//        broadcast("connect");
    }

    
    
    
    @OnClose
    public void onClose(Session userSession) {
        userSessions.remove(userSession);
        
    }

    @OnMessage
    public void onMessage(String message, Session userSession) {
        broadcast(message);
    }

    public static void broadcast(String msg) {
        for (Session session : userSessions) {
            session.getAsyncRemote().sendText(msg);
        }
    }

}
    
    
    
 /*   *//***
     * 웹 소켓이 연결되면 호출되는 이벤트
     *//*
    @OnOpen
    public void handleOpen(Session userSession){
    	userSessions.add(userSession);
        System.out.println("client is now connected...");
    }
    *//**
     * 웹 소켓으로부터 메시지가 오면 호출되는 이벤트
     * @param message
     * @return
     *//*
    @OnMessage
    public String handleMessage(String message, Session session) throws IOException{
    	 System.out.println("receive from client : "+message);
        
    	
			// Iterate over the connected sessions
			// and broadcast the received message
			for (Session client : clients) {
				if (!client.equals(session)) {
					client.getBasicRemote().sendText(message);
				}
		
    }return message;
    	 }
    *//**
     * 웹 소켓이 닫히면 호출되는 이벤트
     *//*
    @OnClose
    public void handleClose(Session session){
    	clients.remove(session);
        System.out.println("client is now disconnected...");
    }
    *//**
     * 웹 소켓이 에러가 나면 호출되는 이벤트
     * @param t
     *//*
    @OnError
    public void handleError(Throwable t){
        t.printStackTrace();
    }*/



