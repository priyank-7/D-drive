package com.cloude;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.cloude.Token.TokenManager;
import com.cloude.db.MongoDBConnection;
import com.cloude.db.User;
import com.cloude.db.UserDAO;
import com.cloude.headers.Request;
import com.cloude.headers.Response;
import com.cloude.headers.StatusCode;

public class LoadBalancer {
    private final ServerSocket serverSocket;
    private final UserDAO userDAO;
    private final TokenManager tokenManager;
    private final ExecutorService threadPool;

    private final List<InetSocketAddress> storageNodes = new ArrayList<>();
    private int currentIndex = 0;

    // TODO
    /*
     * Track storage nodes in load balancer.
     * Assign unique Id to Each storage node to identify them.
     * Impliment functionality to send request to load balancer and peer node when
     * any storage node spinup.
     * Periodically put check on storage nodes to check if they are alive or storage
     * node send heartbeat to load balancer.
     * Store unique Id of storage node in each storage node, so that it is usefull
     * in sending heart beat to load balancer and peer node.
     * Impliment peer to peer communication between storage nodes and each node
     * should have track of other alive nodes.
     * Implimnet logic of replication of data between storage nodes in storage node
     * module.
     * Impliment logic to remove dead storage node from load balancer and peer
     * storage nodes.
     * Decide How to store files in storage nodes.
     * Put a time to leave on generated tokens.
     */

    public LoadBalancer(int port) {
        try {
            serverSocket = new ServerSocket(port);
            userDAO = new UserDAO(MongoDBConnection.getDatabase("ddrive"));
            tokenManager = new TokenManager();
            int poolSize = Runtime.getRuntime().availableProcessors(); // Or any other number based on your load
            System.out.println("Pool size: " + poolSize);
            this.threadPool = Executors.newFixedThreadPool(poolSize);
            // Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
            storageNodes.add(new InetSocketAddress("localhost", 9090));
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize LoadBalancer", e);
        }
    }

    public void start() {
        while (true) {
            try {
                Socket clientSocket = serverSocket.accept();
                threadPool.submit(new ClientHandler(clientSocket));
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
                        case VALIDATE_TOKEN:
                            handleValidateToken(request);
                            break;
                        case DISCONNECT:
                            clientSocket.close();
                            return;
                        default:
                            Response response = new Response(StatusCode.UNKNOWN_REQUEST, "Unknown request type");
                            out.writeObject(response);
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
                user = userDAO.getUserByUsername(username);
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
                out.flush();
            }
        }

        private void handleForwardRequest(Request request) throws IOException {
            String token = (String) request.getToken();
            // System.out.println("[LoadBalancer]: Validation for forward request");
            if (tokenManager.validateToken(token)) {
                InetSocketAddress storageNode = selectStorageNode();
                Response response = Response.builder()
                        .statusCode(StatusCode.SUCCESS)
                        .payload(storageNode)
                        .build();
                out.writeObject(response);
            } else {
                Response response = new Response(StatusCode.AUTHENTICATION_FAILED, "Invalid or expired token");
                out.writeObject(response);
            }
        }

        private InetSocketAddress selectStorageNode() {
            synchronized (this) {
                if (storageNodes.isEmpty()) {
                    return null;
                }
                // Round-robin selection with thread safety
                InetSocketAddress selectedNode = storageNodes.get(currentIndex);
                currentIndex = (currentIndex + 1) % storageNodes.size();
                return selectedNode;
            }
        }

        private void handleValidateToken(Request request) throws IOException {
            String token = request.getToken();
            boolean isValid = tokenManager.validateToken(token); // impliment different for storage node
            // System.out.println("[LoadBalancer]: Is token valid " + isValid);

            Response response;
            if (isValid) {
                response = Response.builder()
                        .statusCode(StatusCode.SUCCESS)
                        .payload("Token is valid")
                        .build();
            } else {
                response = Response.builder()
                        .statusCode(StatusCode.AUTHENTICATION_FAILED)
                        .payload("Token is invalid or expired")
                        .build();
            }
            out.writeObject(response);
        }

        // public void shutdown() {
        // System.out.println("Shutting down server...");
        // threadPool.shutdown();
        // try {
        // if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
        // threadPool.shutdownNow();
        // }
        // } catch (InterruptedException ex) {
        // threadPool.shutdownNow();
        // Thread.currentThread().interrupt();
        // }

        // // Close the server socket
        // try {
        // serverSocket.close();
        // } catch (IOException e) {
        // e.printStackTrace();
        // }

        // System.out.println("Server shut down.");
        // }
    }
}