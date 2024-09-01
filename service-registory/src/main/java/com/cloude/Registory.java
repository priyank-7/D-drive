package com.cloude;

import com.cloude.headers.Request;
import com.cloude.headers.RequestType;
import com.cloude.headers.Response;
import com.cloude.headers.StatusCode;
import com.cloude.utilities.NodeInfo;
import com.cloude.utilities.NodeStatus;
import com.cloude.utilities.NodeType;
import com.cloude.utilities.PeerRequest;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Registory {

    // TODO:
    // Retry mechanism logic?
    // Node failure handling?

    private static final int HEARTBEAT_INTERVAL = 5000; // 5 seconds
    private static final int HEARTBEAT_TIMEOUT = 2000; // 2 seconds
    private static final int MAX_RETRIES = 3;

    private final ServerSocket serverSocket;
    private final ExecutorService threadPool;

    private static final ConcurrentHashMap<String, NodeInfo> storageNodes = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, NodeInfo> loadBalancers = new ConcurrentHashMap<>();

    public Registory(int port) throws IOException {
        this.serverSocket = new ServerSocket(port);
        int poolSize = Runtime.getRuntime().availableProcessors();
        this.threadPool = Executors.newFixedThreadPool(poolSize);
        System.out.println("[Registory] :" + "Server started on port " + port);
        startHeartbeatThread();
    }

    public void start() {
        System.out.println("Service Registry is running...");
        while (true) {
            try {
                Socket clientSocket = serverSocket.accept();
                threadPool.submit(new ServiceHandler(clientSocket));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void stop() {
        try {
            serverSocket.close();
            threadPool.shutdown();
            System.out.println("Service Registry stopped.");
        } catch (IOException e) {
            System.err.println("Error closing server socket: " + e.getMessage());
        }
    }

    private void startHeartbeatThread() {
        System.out.println("[Registory] :" + "Starting heartbeat thread...");
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        System.out.println("[Registory] :" + "Heartbeat interval: " + HEARTBEAT_INTERVAL + "ms");
        scheduler.scheduleAtFixedRate(this::sendHeartbeats, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);
    }

    private void sendHeartbeats() {
        storageNodes.values().forEach(this::sendHeartbeat);
        loadBalancers.values().forEach(this::sendHeartbeat);
    }

    private void sendHeartbeat(NodeInfo node) {
        try (Socket socket = new Socket(node.getNodeAddress().getHostName(), node.getNodeAddress().getPort())) {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

            // Send PING request
            // PeerRequest request = new PeerRequest(RequestType.PING);
            Request request = new Request(RequestType.PING);
            out.writeObject(request);
            out.flush();
            System.out.println("[Registory] :" + "Sent PING to node " + node.getNodeId());

            // Wait for a response within the timeout
            socket.setSoTimeout(HEARTBEAT_TIMEOUT);
            Response response = (Response) in.readObject();

            if (response.getStatusCode() == StatusCode.PONG) {
                node.setStatus(NodeStatus.ACTIVE);
                node.setLastResponse(new Date());
                node.setFailedAttempts(0);
            } else {
                handleNodeFailure(node);
            }

        } catch (IOException | ClassNotFoundException e) {
            handleNodeFailure(node);
        }
    }

    private void handleNodeFailure(NodeInfo node) {
        int failedAttempts = node.getFailedAttempts() + 1;
        node.setFailedAttempts(failedAttempts);

        if (failedAttempts >= MAX_RETRIES) {
            if (node.getNodetype().equals(NodeType.STORAGE_NODE)) {
                storageNodes.remove(node.getNodeId());
            } else {
                loadBalancers.remove(node.getNodeId());
            }
            System.out.println("[Registory] :" + "Node " + node.getNodeId() + " is unresponsive.");
        } else {
            node.setStatus(NodeStatus.INACTIVE);
            System.out.println("[Registory] :" + "Node " + node.getNodeId() + " is inactive (Attempt " + failedAttempts
                    + "/" + MAX_RETRIES + ").");
        }
    }

    // Handle service connections
    private class ServiceHandler implements Runnable {
        private final Socket clientSocket;
        private ObjectOutputStream out;
        private ObjectInputStream in;

        public ServiceHandler(Socket clientSocket) {
            this.clientSocket = clientSocket;
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
                    PeerRequest request = (PeerRequest) in.readObject();

                    switch (request.getRequestType()) {
                        case REGISTER:
                            handleRegisterRequest(request);
                            break;
                        case UNREGISTER:
                            handleUnregisterRequest(request);
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
                System.err.println("Error handling service: " + e.getMessage());
            }
        }

        private void handleRegisterRequest(PeerRequest request) throws IOException {
            NodeInfo nodeInfo = NodeInfo.builder()
                    .nodeId(UUID.randomUUID().toString())
                    .nodetype(request.getNodeType())
                    .nodeAddress(request.getSocketAddress())
                    .status(NodeStatus.ACTIVE)
                    .registrationTime(new Date())
                    .lastResponse(new Date())
                    .build();
            System.out.println("[Registory] :" + "Node registered: " + nodeInfo);
            Response response;
            if (request.getNodeType() == NodeType.STORAGE_NODE) {
                storageNodes.put(nodeInfo.getNodeId(), nodeInfo);
                response = new Response(StatusCode.SUCCESS, "Node registered successfully");
            } else if (request.getNodeType() == NodeType.LOAD_BALANCER) {
                loadBalancers.put(nodeInfo.getNodeId(), nodeInfo);
                response = new Response(StatusCode.SUCCESS, "Node registered successfully");
            } else {
                response = new Response(StatusCode.INTERNAL_SERVER_ERROR, "Unknown request type");
            }
            out.writeObject(response);
            out.flush();
        }

        private void handleUnregisterRequest(PeerRequest request) throws IOException {
            Response response;
            if (request.getNodeType() == NodeType.STORAGE_NODE) {
                storageNodes.remove(request.getSocketAddress().toString());
                response = new Response(StatusCode.SUCCESS, "Node unregistered successfully");
            } else if (request.getNodeType() == NodeType.LOAD_BALANCER) {
                loadBalancers.remove(request.getSocketAddress().toString());
                response = new Response(StatusCode.SUCCESS, "Node unregistered successfully");
            } else {
                response = new Response(StatusCode.INTERNAL_SERVER_ERROR, "Unknown request type");
            }
            out.writeObject(response);
            out.flush();
        }

    }
}
