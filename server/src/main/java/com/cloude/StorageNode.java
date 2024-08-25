package com.cloude;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

import com.cloude.headers.Request;
import com.cloude.headers.RequestType;
import com.cloude.headers.Response;
import com.cloude.headers.StatusCode;

public class StorageNode {
    private ServerSocket serverSocket;
    private static final String LOAD_BALANCER_HOST = "localhost";
    private static final int LOAD_BALANCER_PORT = 8080;

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
            try (Socket loadBalancerSocket = new Socket(LOAD_BALANCER_HOST, LOAD_BALANCER_PORT);
                    ObjectOutputStream loadBalancerOut = new ObjectOutputStream(loadBalancerSocket.getOutputStream());
                    ObjectInputStream loadBalancerIn = new ObjectInputStream(loadBalancerSocket.getInputStream())) {

                // Send the token to the load balancer for validation
                Request validationRequest = Request.builder()
                        .requestType(RequestType.VALIDATE_TOKEN)
                        .token(token)
                        .build();
                loadBalancerOut.writeObject(validationRequest);

                // Receive the response from the load balancer
                Response response = (Response) loadBalancerIn.readObject();

                return response.getStatusCode() == StatusCode.SUCCESS;

            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
                return false;
            }
        }
    }

    public static void main(String[] args) {
        StorageNode storageNode = new StorageNode(9090);
        storageNode.start();
    }
}
