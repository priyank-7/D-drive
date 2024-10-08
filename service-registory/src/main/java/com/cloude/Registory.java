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
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Date;
import java.util.UUID;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.core.LoggerContext;

// TODO: data security while transferring data between nodes and while stored

public class Registory {

    // TODO:
    /*
     * Retry mechanism logic?
     * Node failure handling?
     * Impliment logic to detect already existing nodes, which are spin up before
     * the registory server.
     */

    // LoggerContext context = (LoggerContext) LogManager.getLogger();
    // private org.apache.logging.log4j.core.Logger logger =
    // context.getLogger(Registory.class.getName());

    org.apache.logging.log4j.core.Logger logger = LoggerContext.getContext().getLogger(Registory.class.getName());

    private static final int HEARTBEAT_INTERVAL = 5000; // 5 seconds
    private static final int HEARTBEAT_TIMEOUT = 2000; // 2 seconds
    private static final int MAX_RETRIES = 3;

    private final ServerSocket serverSocket;
    private final ExecutorService threadPool;

    private static final ConcurrentHashMap<String, NodeInfo> storageNodes = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, NodeInfo> loadBalancers = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, BlockingQueue<String>> messagingQueues = new ConcurrentHashMap<>();

    public Registory(int port) throws IOException {
        this.logger.setLevel(org.apache.logging.log4j.Level.INFO);
        this.serverSocket = new ServerSocket(port);
        int poolSize = Runtime.getRuntime().availableProcessors();
        this.threadPool = Executors.newFixedThreadPool(poolSize);
        this.logger.info("Server started on port " + port);
        startHeartbeatThread();
    }

    public void start() {
        logger.info("Service Registry is running...");
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
            this.logger.info("Service Registry stopped.");
        } catch (IOException e) {
            e.printStackTrace();
            this.logger.error("Error closing server socket");
        }
    }

    private void startHeartbeatThread() {
        this.logger.info("Starting heartbeat thread...");
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        this.logger.info("Heartbeat interval: " + HEARTBEAT_INTERVAL + "ms");
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

            Request request = new Request(RequestType.PING);
            out.writeObject(request);
            out.flush();
            this.logger.info("Sent PING to node " + node.getNodeId());

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

        if (failedAttempts == MAX_RETRIES) {
            this.logger.error("Failed attempts: " + failedAttempts + " Node " + node.getNodeId() + " is unresponsive.");
            // TODO: keep track of failed attempts and mark the node as inactive, If node
            // change happens the no need to send the inactive node to the Load Balancer.
            sendActiveNodesToLoadBalancers();
        } else if (failedAttempts > MAX_RETRIES) {
            long currentTime = new Date().getTime();
            long lastResponseTime = node.getLastResponse().getTime();
            if (currentTime - lastResponseTime > 900000) { // 15 minutes in milliseconds
                storageNodes.remove(node.getNodeId());

                // BUG: node is removed from the map before 15 minutes
                // TODO: remove messaging queue as well
                logger.info("Node " + node.getNodeId() + " is removed from the registry.");
            }
        } else {
            node.setStatus(NodeStatus.INACTIVE);
            this.logger.error(
                    "Failed attempts for node" + node.getNodeId() + "(" + failedAttempts + "/" + MAX_RETRIES + ")");
        }
    }

    private void sendActiveNodesToLoadBalancers() {
        List<SocketAddress> activeStorageNodes = new ArrayList<>();
        for (NodeInfo storageNode : storageNodes.values()) {
            if (storageNode.getStatus() == NodeStatus.ACTIVE) {
                activeStorageNodes.add(storageNode.getNodeAddress());
            }
        }

        for (NodeInfo loadBalancer : loadBalancers.values()) {
            try (Socket lbSocket = new Socket(loadBalancer.getNodeAddress().getAddress(),
                    loadBalancer.getNodeAddress().getPort())) {
                ObjectOutputStream lbOut = new ObjectOutputStream(lbSocket.getOutputStream());
                Request lbRequest = new Request(RequestType.UPDATE, activeStorageNodes);
                lbOut.writeObject(lbRequest);
                lbOut.flush();
            } catch (IOException e) {
                this.logger.error("Failed to send active node list to Load Balancer " + loadBalancer.getNodeId());
            }
        }
    }

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
                        case PUSH_DATA:
                            handlePushDateRequest(request);
                            break;
                        case DELETE_DATA:
                            // TODO: Implement delete data request
                            handleDeleteDateRequest(request);
                        case FORWARD_REQUEST:
                            handleForwardRequest(request);
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
                logger.error("Error handling request");
            }
        }

        private void handleForwardRequest(PeerRequest request) {
            try {
                if (loadBalancers.isEmpty()) {
                    out.writeObject(Response.builder()
                            .statusCode(StatusCode.INTERNAL_SERVER_ERROR)
                            .payload("No active load balancer found")
                            .build());
                    out.flush();
                } else {

                    for (NodeInfo lb : loadBalancers.values()) {
                        out.writeObject(Response.builder()
                                .statusCode(StatusCode.SUCCESS)
                                .payload(lb.getNodeAddress())
                                .build());
                        out.flush();
                        break;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return;
        }

        private void handleRegisterRequest(PeerRequest request) throws IOException {
            NodeInfo nodeInfo;
            Response response;
            InetSocketAddress address = request.getSocketAddress();
            if (request.getNodeType() == NodeType.STORAGE_NODE) {

                // TODO:
                /*
                 * user this.clientsocket.getSocketAdderdd() insted of getSocketAddress()
                 */

                // BUG:
                /*
                 * Get IP and Port of client socket Dynamic insted of getting from request
                 * Make this update for update in data replication, load balancing and
                 */
                nodeInfo = findExistingNode(storageNodes,
                        new InetSocketAddress(this.clientSocket.getInetAddress(), address.getPort()));
                if (nodeInfo != null) {
                    nodeInfo.setStatus(NodeStatus.ACTIVE);
                    nodeInfo.setLastResponse(new Date());
                    logger.info("Storage node re-registered and activated: " + nodeInfo.getNodeId());
                    response = new Response(StatusCode.SUCCESS,
                            "Storage node re-registered and activated successfully");
                } else {
                    nodeInfo = NodeInfo.builder()
                            .nodeId(UUID.randomUUID().toString())
                            .nodetype(request.getNodeType())
                            .nodeAddress(new InetSocketAddress(this.clientSocket.getInetAddress(), address.getPort()))
                            .status(NodeStatus.ACTIVE)
                            .registrationTime(new Date())
                            .lastResponse(new Date())
                            .build();
                    response = new Response(StatusCode.SUCCESS, "Storage node registered successfully");
                }
                storageNodes.put(nodeInfo.getNodeId(), nodeInfo);
                // Adding messaging queue for the storage node
                messagingQueues.computeIfAbsent(nodeInfo.getNodeId(), v -> new LinkedBlockingQueue<>(100));
                response = new Response(StatusCode.SUCCESS, "Storage node registered successfully");
                // Get Messaging queue content from the storage node
                sendActiveNodesToLoadBalancers();

            } else if (request.getNodeType() == NodeType.LOAD_BALANCER) {

                nodeInfo = findExistingNode(loadBalancers,
                        new InetSocketAddress(this.clientSocket.getInetAddress(), address.getPort()));
                if (nodeInfo != null) {
                    nodeInfo.setStatus(NodeStatus.ACTIVE);
                    nodeInfo.setLastResponse(new Date());
                    logger.info("Load Balancer re-registered and activated: " + nodeInfo.getNodeId());
                    response = new Response(StatusCode.SUCCESS,
                            "Load Balancer re-registered and activated successfully");
                } else {
                    nodeInfo = NodeInfo.builder()
                            .nodeId(UUID.randomUUID().toString())
                            .nodetype(request.getNodeType())
                            .nodeAddress(new InetSocketAddress(this.clientSocket.getInetAddress(), address.getPort()))
                            .status(NodeStatus.ACTIVE)
                            .registrationTime(new Date())
                            .lastResponse(new Date())
                            .build();
                    response = new Response(StatusCode.SUCCESS, "Load Balancer registered successfully");
                }
                loadBalancers.put(nodeInfo.getNodeId(), nodeInfo);
                List<InetSocketAddress> activeStorageNodes = new ArrayList<>();
                for (NodeInfo storageNode : storageNodes.values()) {
                    if (storageNode.getStatus() == NodeStatus.ACTIVE) {
                        activeStorageNodes.add(storageNode.getNodeAddress());
                    }
                }
                response = new Response(StatusCode.SUCCESS, activeStorageNodes);
            } else {
                response = new Response(StatusCode.INTERNAL_SERVER_ERROR, "Unknown request type");
            }
            out.writeObject(response);
            out.flush();
            return;
        }

        private NodeInfo findExistingNode(Map<String, NodeInfo> registeredNodes, InetSocketAddress currentAddress) {
            for (NodeInfo node : registeredNodes.values()) {
                if (node.getNodeAddress().equals(currentAddress)
                        && node.getNodeAddress().getPort() == currentAddress.getPort()) {
                    return node;
                }
            }
            return null;
        }

        // TODO: Check whole logic, specially sockety address
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

        private void handlePushDateRequest(PeerRequest request) {

            // BUG: Handle Null response
            String storageNodeId = getStorageNodeId(request.getSocketAddress());
            if (storageNodeId == null) {
                try {
                    out.writeObject(Response.builder()
                            .statusCode(StatusCode.INTERNAL_SERVER_ERROR)
                            .build());
                    out.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return;
            }
            try {

                for (String nodeId : messagingQueues.keySet()) {
                    if (!nodeId.equals(storageNodeId)) {
                        // TODO
                        /*
                         * Add address of the storage from where to get Data.
                         * For that modify the messaging queue to store the address of the storage node
                         */
                        messagingQueues.get(nodeId).put(request.getPayload().toString());
                    }
                }
                out.writeObject(Response.builder()
                        .statusCode(StatusCode.SUCCESS)
                        .build());
                out.flush();
                System.out.println(messagingQueues);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }

        }

        private void handleDeleteDateRequest(PeerRequest request) {
            // TODO: Implement delete data request
        }

        private String getStorageNodeId(InetSocketAddress address) {
            for (NodeInfo node : storageNodes.values()) {
                if (node.getNodeAddress().equals(address)) {
                    return node.getNodeId();
                }
            }
            return null;
        }
    }
}
