package com.cloude.server;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

import com.cloude.headers.Request;
import com.cloude.headers.Response;
import com.cloude.headers.StatusCode;

public class StorageNode {
    private ServerSocket serverSocket;

    public StorageNode(int port) {
        try {
            serverSocket = new ServerSocket(port);
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
                    String token = (String) request.getToken();

                    if (validateTokenWithLoadBalancer(token)) {
                        // Process request
                        Response response = new Response(StatusCode.SUCCESS, "Request processed by storage node");
                        out.writeObject(response);
                    } else {
                        Response response = new Response(StatusCode.AUTHENTICATION_FAILED, "Invalid or expired token");
                        out.writeObject(response);
                    }
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        private boolean validateTokenWithLoadBalancer(String token) {
            // TODO
            // In a real implementation, this would involve communicating with the load
            // balancer
            // For now, we'll simulate this by assuming the token is valid if it's not null
            return token != null;
        }
    }

    public static void main(String[] args) {
        StorageNode storageNode = new StorageNode(9090);
        storageNode.start();
    }
}
