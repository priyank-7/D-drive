package com.cloude;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

import com.cloude.Token.TokenManager;
import com.cloude.db.User;
import com.cloude.db.UserDAO;
import com.cloude.headers.Request;
import com.cloude.headers.Response;
import com.cloude.headers.StatusCode;

public class LoadBalancer {
    private ServerSocket serverSocket;
    private UserDAO userDAO;
    private TokenManager tokenManager;

    public LoadBalancer(int port) {
        try {
            serverSocket = new ServerSocket(port);
            userDAO = new UserDAO();
            tokenManager = new TokenManager();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start() {
        while (true) {
            try {
                Socket clientSocket = serverSocket.accept();
                new Thread(new ClientHandler(clientSocket)).start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private class ClientHandler implements Runnable {
        private Socket clientSocket;
        private ObjectOutputStream out;
        private ObjectInputStream in;

        public ClientHandler(Socket socket) {
            this.clientSocket = socket;
            try {
                out = new ObjectOutputStream(clientSocket.getOutputStream());
                in = new ObjectInputStream(clientSocket.getInputStream());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            try {
                while (true) {
                    Request request = (Request) in.readObject();

                    switch (request.getRequestType()) {
                        case AUTHENTICATE:
                            handleAuthenticate(request);
                            break;
                        case FORWARD_REQUEST:
                            handleForwardRequest(request);
                            break;
                        case DISCONNECT:
                            clientSocket.close();
                            return;
                        default:
                            break;
                    }
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        private void handleAuthenticate(Request request) throws IOException {
            String[] credentials = ((String) request.getPayload()).split(":");
            String username = credentials[0];
            String password = credentials[1];
            User user;
            try {
                user = userDAO.getUser(username);
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }

            if (user != null && user.getPasswordHash().equals(password)) {
                String token = tokenManager.generateToken(username);
                Response response = Response.builder()
                        .statusCode(StatusCode.SUCCESS)
                        .payload(token)
                        .build();
                out.writeObject(response);
            } else {
                Response response = Response.builder()
                        .statusCode(StatusCode.AUTHENTICATION_FAILED)
                        .payload("Invalid credentials")
                        .build();
                out.writeObject(response);
            }
        }

        private void handleForwardRequest(Request request) throws IOException {
            String token = (String) request.getToken();

            if (tokenManager.validateToken(token)) {

                // TODO
                // Forward request to appropriate storage node
                // Code to select and forward to storage node goes here
                Response response = new Response(StatusCode.SUCCESS, "Request forwarded to storage node");
                out.writeObject(response);
            } else {
                Response response = new Response(StatusCode.AUTHENTICATION_FAILED, "Invalid or expired token");
                out.writeObject(response);
            }
        }
    }

    public static void main(String[] args) {
        LoadBalancer loadBalancer = new LoadBalancer(8080);
        loadBalancer.start();
    }
}
