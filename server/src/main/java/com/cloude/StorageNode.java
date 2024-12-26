package com.cloude;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.core.LoggerContext;

import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

import com.cloude.db.MetadataDao;
import com.cloude.db.User;
import com.cloude.headers.Metadata;
import com.cloude.headers.Request;
import com.cloude.headers.RequestType;
import com.cloude.headers.Response;
import com.cloude.headers.StatusCode;
import com.cloude.utilities.NodeType;
import com.cloude.utilities.PeerRequest;
import com.cloude.utilities.ReplicateRequest;

// TODO Priority: Low: data security while transferring data between nodes and while stored 

// TODO
/* 
// Maintain Atomsity while storing/Update data
// Maintain Consistency during data replicateion and deletion, lock or track the file while updating
// Impliment Access Control List (ACL) for file access
// Handle the Failiure of register with registry
// If there is no response from registry, then try to PING Registory on time interval
 */

public class StorageNode {

    org.apache.logging.log4j.core.Logger logger = LoggerContext.getContext().getLogger(StorageNode.class.getName());

    private ServerSocket serverSocket;
    private String SELF_NODE_ID;

    private final String REGISTRY_HOST; // Registry service host
    private final int REGISTRY_PORT; // Registry service port
    private final String STORAGE_DIRECTORY = System.getProperty("user.home") + "/ddrive-storage";
    private long lastPingTime;
    private static final long PING_TIMEOUT = 15000; // 15 seconds
    private static final int LONG_POLLING_TIMEOUT = 60000; // 60 seconds
    private static final long POLLING_INTERVAL = 2;

    private final ExecutorService threadPool;
    private ScheduledExecutorService scheduler;
    private MetadataDao metadataDao;
    private static final ConcurrentHashMap<String, ReplicateRequest> pushReplicationData = new ConcurrentHashMap<>();
    private final BlockingQueue<ReplicateRequest> pullReplicationQueue;

    public StorageNode(int port, String registryIP, int registryPort) {

        this.REGISTRY_HOST = registryIP;
        this.REGISTRY_PORT = registryPort;

        this.logger.setLevel(org.apache.logging.log4j.Level.TRACE);
        this.threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        this.pullReplicationQueue = new LinkedBlockingQueue<>();
        this.scheduler = Executors.newScheduledThreadPool(4); // Single thread for scheduled tasks

        try {
            serverSocket = new ServerSocket(port);
            int poolSize = Runtime.getRuntime().availableProcessors();
            metadataDao = new MetadataDao("D-drive", "metadata");
            logger.info("Thread pool size: " + poolSize);
            if (!registerWithRegistry()) {
                logger.error("Failed to register with the Registry");
                throw new Exception("Failed to register with the Registry");
            }
            logger.info("Successfully registered with the Registry");
            startPullDataTask(); // Start the scheduled task
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void start() {
        logger.info("Listening on port " + serverSocket.getLocalPort());
        this.threadPool.submit(new Runnable() {
            @Override
            public void run() {
                makeRelicationRequest();
            }
        });
        while (true) {
            try {
                Socket clientSocket = serverSocket.accept();
                threadPool.submit(new ClientHandler(clientSocket, this.metadataDao));
                logger.info("Accepted connection from " + clientSocket.getPort());
            } catch (IOException e) {
                this.threadPool.shutdown();
                e.printStackTrace();
            }
        }
    }

    private void startPullDataTask() {
        // Schedule the task to run every second
        this.logger.info("Starting polling thread...");
        this.logger.info("polling interval: " + POLLING_INTERVAL + "s");
        scheduler.scheduleAtFixedRate(this::polling, 2, POLLING_INTERVAL, TimeUnit.SECONDS);
    }

    private void polling() {

        try (Socket registorySocket = new Socket(REGISTRY_HOST, REGISTRY_PORT);
                ObjectOutputStream out = new ObjectOutputStream(registorySocket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(registorySocket.getInputStream())) {

            PeerRequest pullRequest = PeerRequest.builder()
                    .requestType(RequestType.PULL_DATA)
                    .payload(this.SELF_NODE_ID)
                    .build();

            out.writeObject(pullRequest);
            out.flush();

            Response response = (Response) in.readObject();
            if (response.getStatusCode() == StatusCode.SUCCESS) {
                ReplicateRequest pullReplication = (ReplicateRequest) response.getPayload();
                pullReplicationQueue.add(pullReplication);
                logger.debug("Current PullReplicationQueue stare: " + pullReplicationQueue.toString());

                out.writeObject(Response.builder()
                        .statusCode(StatusCode.SUCCESS)
                        .build());
                out.flush();

            } else {
                // logger.error("Failed to pull data from the Registory");
            }

            Thread.sleep(1000);
            out.writeObject(PeerRequest.builder()
                    .requestType(RequestType.DISCONNECT)
                    .build());
            out.flush();

        } catch (IOException | ClassNotFoundException | InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }

    // private void startDataReplication() {
    // new Runnable() {
    // @Override
    // public void run() {
    // makeRelicationRequest();
    // }
    // };
    // }

    private void makeRelicationRequest() { // Copy actual data
        try {
            while (true) {

                System.out.println("Replication Queue Current Status: " + pullReplicationQueue.toString());
                ReplicateRequest currentRequest = this.pullReplicationQueue.take();

                if (currentRequest == null) {
                    logger.error("Current Request is NULL");
                    continue;
                }
                logger.debug("I have to copy file from: " + currentRequest.getAddress().toString());

                String filePath = STORAGE_DIRECTORY + "/" + currentRequest.getFilePath();
                String[] pathParts = currentRequest.getFilePath().split("/");
                User user = getUserDetailsFromLB(pathParts[0]);

                if (currentRequest.getRequestType().equals(RequestType.DELETE_DATA)) {

                    if (this.metadataDao.deleteMetadata(pathParts[1], user.get_id())) {
                        logger.info("Metadata deleted successfully");
                        // TODO send ack
                    } else {
                        logger.error("Failed to delete metadata");
                        // TODO send n_ack
                    }
                    File file = new File(filePath);
                    if (file.exists()) {
                        if (file.delete()) {
                            logger.info("File deleted successfully");
                        } else {
                            logger.error("Failed to delete file");
                        }
                    } else {
                        logger.error("File not found");
                    }
                } else if (currentRequest.getRequestType().equals(RequestType.PUSH_DATA)) {
                    try (Socket registorySocket = new Socket(currentRequest.getAddress().getAddress(),
                            currentRequest.getAddress().getPort());
                            ObjectOutputStream out = new ObjectOutputStream(registorySocket.getOutputStream());
                            ObjectInputStream in = new ObjectInputStream(registorySocket.getInputStream())) {
                        logger.debug("I have to copy file from: " + currentRequest.getAddress().toString());

                        out.writeObject(Request.builder()
                                .requestType(RequestType.COPY_DATA)
                                .payload(currentRequest.getFilePath())
                                .build());
                        out.flush();
                        Response response = (Response) in.readObject();

                        if (response.getStatusCode() == StatusCode.NOT_FOUND) {
                            logger.error("File not found");
                            out.writeObject(new Request(RequestType.DISCONNECT));
                            continue;

                        } else if (response.getStatusCode() == StatusCode.SUCCESS) {

                            Metadata metadata = (Metadata) response.getPayload();

                            logger.info("metada recieved");
                            String directoryPath = STORAGE_DIRECTORY + "/" + pathParts[0];
                            String Current_FilePath = directoryPath + "/" + metadata.getName();
                            logger.info("directory path: " + directoryPath);
                            logger.info("Current file path: " + Current_FilePath);
                            Metadata tempMetadata = this.metadataDao.getMetadata(metadata.getName(), user.get_id());

                            // Metadata metadata = (Metadata) response.getPayload();

                            // logger.info("metada recieved");
                            // String directoryPath = STORAGE_DIRECTORY + "/" + pathParts[0];
                            // String Current_FilePath = directoryPath + "/" + metadata.getName();
                            // Metadata tempMetadata = this.metadataDao.getMetadata(metadata.getName(),
                            // user.get_id());
                            if (tempMetadata == null) {
                                if (this.metadataDao.saveMetadata(metadata)) {
                                    logger.info("Metadata saved ");
                                    out.writeObject(new Response(StatusCode.SUCCESS, "File Created Successfully"));
                                    out.flush();
                                } else {
                                    logger.error("Error to save metadata");
                                    out.writeObject(
                                            new Response(StatusCode.INTERNAL_SERVER_ERROR, "Error to save metadata"));
                                    out.flush();
                                    out.writeObject(new Request(RequestType.DISCONNECT));
                                    // TODO: put current replication request into queue again
                                    continue;
                                }
                            } else {
                                this.metadataDao.updateMetadata(tempMetadata.getName(), user.get_id(),
                                        metadata);
                                logger.info("File details ");
                                out.writeObject(new Response(StatusCode.SUCCESS, "File already exists"));
                            }

                            response = (Response) in.readObject();
                            if (response.getStatusCode().equals(StatusCode.NOT_FOUND)) {
                                logger.error("Actual File not found on peer");
                                out.writeObject(new Request(RequestType.DISCONNECT));
                                // TODO: put current replication request into queue again
                                continue;
                            }

                            File file = new File(Current_FilePath);
                            File directory = new File(STORAGE_DIRECTORY + "/" + pathParts[0]);
                            if (!directory.exists() && !directory.mkdirs()) {
                                logger.error("Failed to create directory with username");
                            }
                            int a = 0;
                            try (FileOutputStream fos = new FileOutputStream(file)) {
                                while (true) {
                                    Response chunkResponse = (Response) in.readObject();
                                    if (StatusCode.EOF.equals(chunkResponse.getStatusCode())) {
                                        logger.debug("EOF received");
                                        fos.close();
                                        break;
                                    }
                                    byte[] chunk = chunkResponse.getData();
                                    int chunkSize = chunkResponse.getDataSize();
                                    fos.write(chunk, 0, chunkSize);
                                    fos.flush();
                                    // logger.debug("iteration: " + a++);
                                    // logger.debug("Receiving chunk of size: " + chunkSize);
                                }
                            } catch (IOException | ClassNotFoundException e) {
                                e.printStackTrace();
                                if (file.exists()) {
                                    file.delete();
                                }
                            }
                        }
                        logger.info("File Success fully replicated from Peer");
                        out.writeObject(new Request(RequestType.DISCONNECT));
                    } catch (IOException | ClassNotFoundException e) {
                        System.out.println(e.getMessage());
                    }

                    // TODO: Send ACK in both the cases
                }
            }
        } catch (Exception e) {
            logger.error("Error in makeRelicationRequest: " + e.getMessage());
        }
    }

    private User getUserDetailsFromLB(String userName) {
        InetSocketAddress lbInetSocketAddress = getLoadBalancerAddress();
        if (lbInetSocketAddress == null) {
            logger.error("LB address receved NULL, returning false");
            return null;
        }
        try (Socket loadBalancerSocket = new Socket(lbInetSocketAddress.getAddress(),
                lbInetSocketAddress.getPort());
                ObjectOutputStream loadBalancerOut = new ObjectOutputStream(loadBalancerSocket.getOutputStream());
                ObjectInputStream loadBalancerIn = new ObjectInputStream(loadBalancerSocket.getInputStream())) {

            // Send the token to the load balancer for validation
            Request validationRequest = Request.builder()
                    .requestType(RequestType.GET_USER_DETAILS)
                    .payload(userName)
                    .build();

            loadBalancerOut.writeObject(validationRequest);
            loadBalancerOut.flush();
            Response response = (Response) loadBalancerIn.readObject();
            loadBalancerOut.writeObject(new Request(RequestType.DISCONNECT));
            if (response.getStatusCode() == StatusCode.SUCCESS) {
                return (User) response.getPayload();
            } else {
                return null;
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    private InetSocketAddress getLoadBalancerAddress() {
        try (Socket socket = new Socket(REGISTRY_HOST, REGISTRY_PORT);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            // Send a request to forward to the correct storage node
            PeerRequest forwardRequest = PeerRequest.builder()
                    .requestType(RequestType.FORWARD_REQUEST)
                    .build();

            out.writeObject(forwardRequest);
            out.flush();
            // logger.info("Request sent to Registory");

            Response response = (Response) in.readObject();
            out.writeObject(new Request(RequestType.DISCONNECT));
            out.flush();

            // logger.info("Response received from Registory " + response.getStatusCode());
            if (response.getStatusCode() == StatusCode.SUCCESS) {
                logger.info("LB Address receved from Registory");
                return (InetSocketAddress) response.getPayload();
            } else {
                logger.warn("Failed to get load balancer address");
                return null;
            }
        } catch (IOException | ClassNotFoundException e) {
            logger.error("Failed to get load balancer address");
            return null;
        }
    }

    private boolean registerWithRegistry() {
        try (Socket registrySocket = new Socket(REGISTRY_HOST, REGISTRY_PORT);
                ObjectOutputStream out = new ObjectOutputStream(registrySocket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(registrySocket.getInputStream())) {

            logger.info("Registering with the Registry...");
            // Create the registration request
            // TODO: Get the actual IP address of the storage node (No need to do at this
            // side, registory will take care)
            PeerRequest regiPeerRequest = PeerRequest.builder()
                    .requestType(RequestType.REGISTER)
                    .nodeType(NodeType.STORAGE_NODE)
                    .socketAddress(new InetSocketAddress("localhost", serverSocket.getLocalPort()))
                    .build();
            out.writeObject(regiPeerRequest);
            out.flush();

            Response response = (Response) in.readObject();
            out.writeObject(PeerRequest.builder()
                    .requestType(RequestType.DISCONNECT)
                    .build());
            out.flush();
            if (response.getStatusCode() == StatusCode.SUCCESS) {
                this.SELF_NODE_ID = (String) response.getPayload();
                logger.info("Node Id: " + this.SELF_NODE_ID);
                return true;
            } else {
                logger.error("Registry registration failed: " + response.getPayload());
                return false;
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return false;
        }
    }

    private class ClientHandler implements Runnable {
        private Socket clientSocket;
        private ObjectOutputStream out;
        private ObjectInputStream in;
        private User currentUser;
        private MetadataDao metadataDao;

        public ClientHandler(Socket socket, MetadataDao metadataDao) {
            this.clientSocket = socket;
            this.metadataDao = metadataDao;
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
                    String token = request.getToken();

                    if (request.getRequestType() == RequestType.ASK) {
                        handleAskRequest(request);
                        return;
                    }
                    if (request.getRequestType() == RequestType.PING) {
                        handlePingRequest();
                        clientSocket.close();
                        return;
                    }
                    if (request.getRequestType() == RequestType.ACK_REPLICATION) {
                        handleAckRepplication(request);
                        return;
                    }
                    if (request.getRequestType() == RequestType.COPY_DATA) {
                        handleCopyData(request);
                        return;
                    }

                    if (validateTokenWithLoadBalancer(token)) {
                        switch (request.getRequestType()) {
                            case UPLOAD_FILE:
                                handleUploadFile(request);
                                break;
                            case DOWNLOAD_FILE:
                                handleDownloadFile(request);
                                break;
                            case DELETE_FILE:
                                handleDeleteFile(request);
                                break;
                            case GET_METADATA:
                                handleGetMetadata(request);
                                break;
                            case DISCONNECT:
                                logger.info("Disconnecting client");
                                clientSocket.close();
                                return;
                            default:
                                Response response = new Response(StatusCode.UNKNOWN_REQUEST, "Unknown request type");
                                out.writeObject(response);
                        }
                    } else {
                        Response response = new Response(StatusCode.AUTHENTICATION_FAILED, "Invalid or expired token");
                        out.writeObject(response);
                    }
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (!clientSocket.isClosed()) {
                        out.close();
                        in.close();
                        clientSocket.close(); // Ensure the socket is closed in case of exceptions
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        private void handleAskRequest(Request request) {
            logger.debug("Inside handle Ask request");
        }

        private void handlePingRequest() throws IOException {
            try {
                // Send a simple PONG response back to the client
                out.writeObject(new Response(StatusCode.PONG, "PONG"));
                out.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void handleAckRepplication(Request request) throws IOException {
            String replicationId = request.getPayload().toString();
            try {
                pushReplicationData.remove(replicationId);
                logger.info("Replication ID removed from the list " + replicationId);
            } catch (NullPointerException e) {
                logger.error("Value with repliation ID not found" + replicationId);
            }
            out.writeObject(new Response(StatusCode.SUCCESS));
            out.flush();
            return;
        }

        private boolean validateTokenWithLoadBalancer(String token) {

            InetSocketAddress lbInetSocketAddress = getLoadBalancerAddress();
            if (lbInetSocketAddress == null) {
                logger.error("LB address receved NULL, returning false");
                return false;
            }
            try (Socket loadBalancerSocket = new Socket(lbInetSocketAddress.getAddress(),
                    lbInetSocketAddress.getPort());
                    ObjectOutputStream loadBalancerOut = new ObjectOutputStream(loadBalancerSocket.getOutputStream());
                    ObjectInputStream loadBalancerIn = new ObjectInputStream(loadBalancerSocket.getInputStream())) {

                // Send the token to the load balancer for validation
                Request validationRequest = Request.builder()
                        .requestType(RequestType.VALIDATE_TOKEN)
                        .token(token)
                        .build();
                loadBalancerOut.writeObject(validationRequest);
                loadBalancerOut.flush();
                Response response = (Response) loadBalancerIn.readObject();
                loadBalancerOut.writeObject(new Request(RequestType.DISCONNECT));
                if (response.getStatusCode() == StatusCode.SUCCESS) {
                    this.currentUser = (User) response.getPayload();

                    return true;
                } else {
                    return false;
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
                return false;
            }
        }

        // Handle Upload File Request

        private void handleUploadFile(Request request) throws IOException {
            logger.info("uploade file request received");
            Metadata metadata = (Metadata) request.getPayload();
            logger.info("metada recieved");
            String directoryPath = STORAGE_DIRECTORY + "/" + this.currentUser.getUsername();
            String filePath = directoryPath + "/" + metadata.getName();
            Metadata tempMetadata = this.metadataDao.getMetadata(metadata.getName(), this.currentUser.get_id());
            if (tempMetadata == null) {
                tempMetadata = Metadata.builder()
                        .name(metadata.getName())
                        .size(metadata.getSize())
                        .isFolder(false)
                        .path(filePath)
                        .owner(this.currentUser.get_id())
                        .createdDate(new Date())
                        .modifiedDate(new Date())
                        .build();
                if (this.metadataDao.saveMetadata(tempMetadata)) {
                    logger.info("Metadata saved ");
                    out.writeObject(new Response(StatusCode.SUCCESS, "File Created Successfully"));
                    out.flush();
                } else {
                    logger.error("Error to save metadata");
                    out.writeObject(new Response(StatusCode.INTERNAL_SERVER_ERROR, "Error to save metadata"));
                    out.flush();
                }
            } else {
                tempMetadata.setModifiedDate(new Date());
                tempMetadata.setSize(metadata.getSize());
                this.metadataDao.updateMetadata(tempMetadata.getName(), this.currentUser.get_id(), tempMetadata);
                logger.info("File details ");
                out.writeObject(new Response(StatusCode.SUCCESS, "File already exists"));
            }
            logger.info("Ready to receive file");
            File file = new File(filePath);
            File directory = new File(directoryPath);
            if (!directory.exists() && !directory.mkdirs()) {
                logger.info("Failed to create directory with username");
            }
            try (FileOutputStream fos = new FileOutputStream(file)) {
                while (true) {
                    Response chunkResponse = (Response) in.readObject();
                    if (StatusCode.EOF.equals(chunkResponse.getStatusCode())) {
                        fos.close();
                        break;
                    }
                    byte[] chunk = chunkResponse.getData();
                    int chunkSize = chunkResponse.getDataSize();
                    fos.write(chunk, 0, chunkSize);
                    fos.flush();
                }

                // send data to registory for replication
                ReplicateRequest pushRequest = ReplicateRequest.builder()
                        .replicationId(UUID.randomUUID().toString())
                        .filePath(this.currentUser.getUsername() + "/" + metadata.getName())
                        .NodeId(SELF_NODE_ID)
                        .build();

                pushReplicationData.put(pushRequest.getReplicationId(), pushRequest);
                pushReplicationDataToRegistory(pushRequest, RequestType.PUSH_DATA);
                // TODO: based on the method response can impliment retry mechanism for
                // replication

                out.writeObject(new Response(StatusCode.SUCCESS, "File uploaded successfully"));
                out.flush();
                return;
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
                if (file.exists()) {
                    file.delete();
                }
                out.writeObject(new Response(StatusCode.INTERNAL_SERVER_ERROR, "Failed to upload file"));
                return;
            }
        }

        // Handle Download File Request

        private void handleDownloadFile(Request request) throws IOException {
            String fileName = (String) request.getPayload(); // Retrieving the filename from payload

            Metadata tempMetadata = this.metadataDao.getMetadata(fileName, this.currentUser.get_id());
            if (tempMetadata == null) {
                Response response = new Response(StatusCode.NOT_FOUND, "File not found");
                out.writeObject(response);
                out.flush();
                return;
            }
            File file = new File(
                    STORAGE_DIRECTORY + "/" + this.currentUser.getUsername() + "/" + tempMetadata.getName());

            if (file.exists()) {
                int chunkSize = 4096;
                byte[] buffer = new byte[chunkSize];
                Response response;

                // Send file metadata before sending the file data
                Metadata metadata = Metadata.builder()
                        .name(file.getName())
                        .size(file.length())
                        .isFolder(false)
                        .build();
                response = Response.builder()
                        .statusCode(StatusCode.SUCCESS)
                        .payload(metadata)
                        .build();
                out.writeObject(response);
                out.flush();

                // Wait for client acknowledgment of the metadata
                try {
                    Response clientResponse = (Response) in.readObject();
                    if (clientResponse.getStatusCode() == StatusCode.SUCCESS) {
                        logger.info("Client acknowledged metadata receipt.");
                    } else {
                        logger.error("Client failed to acknowledge metadata receipt.");
                        return;
                    }
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                    return;
                }

                // Now send the file data in chunks
                try (FileInputStream fis = new FileInputStream(file)) {
                    int bytesRead;
                    while ((bytesRead = fis.read(buffer)) != -1) {
                        Response chunkResponse = Response.builder()
                                .statusCode(StatusCode.SUCCESS)
                                .payload(fileName)
                                .data(Arrays.copyOf(buffer, bytesRead)) // Send only the valid bytes
                                .dataSize(bytesRead)
                                .build();
                        out.writeObject(chunkResponse);
                        out.flush();
                    }
                    // Signal the end of the file transmission
                    response = Response.builder()
                            .statusCode(StatusCode.EOF)
                            .build();
                    out.writeObject(response);
                    out.flush();

                } catch (IOException e) {
                    e.printStackTrace();
                    response = new Response(StatusCode.INTERNAL_SERVER_ERROR, "Failed to read file");
                    out.writeObject(response);
                    out.flush();
                }
            } else {
                Response response = new Response(StatusCode.NOT_FOUND, "File not found");
                out.writeObject(response);
                out.flush();
            }
        }

        private void handleDeleteFile(Request request) throws IOException {
            String fileName = (String) request.getPayload();
            Metadata tempMetaData = this.metadataDao.getMetadata(fileName, this.currentUser.get_id());
            if (tempMetaData == null) {
                logger.warn("File not found");
                Response response = new Response(StatusCode.NOT_FOUND, "File not found");
                out.writeObject(response);
                out.flush();
                return;
            }
            if (!this.metadataDao.deleteMetadata(tempMetaData)) {
                logger.error("Failed to delete metadata");
                Response response = new Response(StatusCode.INTERNAL_SERVER_ERROR, "Failed to delete metadata");
                out.writeObject(response);
                out.flush();
                return;
            }
            File file = new File(
                    STORAGE_DIRECTORY + "/" + this.currentUser.getUsername() + "/" + tempMetaData.getName());
            if (file.exists()) {
                if (file.delete()) {

                    // send data to registory for replication
                    ReplicateRequest pushRequest = ReplicateRequest.builder()
                            .replicationId(UUID.randomUUID().toString())
                            .NodeId(SELF_NODE_ID)
                            .filePath(this.currentUser.getUsername() + "/" + tempMetaData.getName())
                            .build();

                    pushReplicationData.put(pushRequest.getReplicationId(), pushRequest);
                    pushReplicationDataToRegistory(pushRequest, RequestType.DELETE_DATA);

                    // TODO: based on the method response impliment retry mechanism for replication

                    Response response = new Response(StatusCode.SUCCESS, "File deleted successfully");
                    out.writeObject(response);
                    out.flush();

                } else {
                    Response response = new Response(StatusCode.INTERNAL_SERVER_ERROR, "Failed to delete file");
                    out.writeObject(response);
                    out.flush();
                }
            } else {
                logger.info("File not found");
                Response response = new Response(StatusCode.NOT_FOUND, "File not found");
                out.writeObject(response);
                out.flush();
            }
            out.flush();
        }

        // private void handleCopyData(Request request) {

        // try {
        // String fileName = (String) request.getPayload();
        // String filePath = STORAGE_DIRECTORY + "/" + request.getPayload().toString();
        // String[] pathParts = request.getPayload().toString().split("/", 2);
        // User user = getUserDetailsFromLB(pathParts[0]);
        // Metadata tempMetadata = this.metadataDao.getMetadata(pathParts[1],
        // user.get_id());
        // if (tempMetadata == null) {
        // logger.error("Metadata not found");
        // out.writeObject(new Response(StatusCode.NOT_FOUND, "Metadata not found"));
        // return;
        // }
        // out.writeObject(Response.builder()
        // .statusCode(StatusCode.SUCCESS)
        // .payload(tempMetadata)
        // .build());
        // out.flush();

        // Response response = (Response) in.readObject();
        // if (response.getStatusCode().equals(StatusCode.INTERNAL_SERVER_ERROR)) {
        // logger.info("Not a Success Respoonse from Peer Server on data copy");
        // return;
        // }
        // // filePath = filePath.replace("/", "\\").trim(); // Replace slashes and trim
        // // whitespace
        // File file = new File(filePath);
        // logger.info("file path from peer: " + filePath);
        // logger.info("file path from peer: " + file.getPath());
        // if (!file.exists()) {
        // logger.debug("File not found");
        // out.writeObject(new Response(StatusCode.NOT_FOUND));
        // out.flush();
        // return;
        // }
        // logger.debug("file found");
        // if (file.exists()) {
        // logger.debug("if file exits block");
        // int chunkSize = 4096;
        // byte[] buffer = new byte[chunkSize];

        // // Now send the file data in chunks
        // try (FileInputStream fis = new FileInputStream(file)) {
        // int bytesRead;
        // logger.debug("file found and ready to send");
        // while ((bytesRead = fis.read(buffer)) != -1) {
        // response = Response.builder()
        // .statusCode(StatusCode.SUCCESS)
        // // .payload(fileName)
        // .data(Arrays.copyOf(buffer, bytesRead)) // Send only the valid bytes
        // .dataSize(bytesRead)
        // .build();
        // out.writeObject(response);
        // out.flush();
        // // Thread.sleep(100);
        // }

        // // Signal the end of the file transmission
        // fis.close();
        // logger.debug("EOF block");
        // response = Response.builder()
        // .statusCode(StatusCode.EOF)
        // .build();
        // out.writeObject(response);
        // out.flush();
        // logger.debug("file sent successfully");

        // } catch (IOException e) {
        // e.printStackTrace();
        // response = new Response(StatusCode.INTERNAL_SERVER_ERROR, "Failed to read
        // file");
        // out.writeObject(response);
        // out.flush();
        // }
        // }
        // } catch (IOException | ClassNotFoundException e) {
        // logger.debug("catch block after file found");
        // e.printStackTrace();
        // }
        // return;
        // }
        private void handleCopyData(Request request) {

            try {

                String fileName = (String) request.getPayload();
                logger.info("File name: " + fileName);

                String[] pathParts = request.getPayload().toString().split("/", 2);
                logger.info("Path Parts: ", pathParts.toString());
                User user = getUserDetailsFromLB(pathParts[0]);
                logger.info("PathPart: " + pathParts[0]);
                logger.info("file path: " + pathParts[1]);

                Path fullPath = Paths.get(STORAGE_DIRECTORY, pathParts[0], pathParts[1]);
                logger.debug("Full Path: " + fullPath.toString());

                String filePath = STORAGE_DIRECTORY + "/" + pathParts[0];
                // String filePath = "/Users/priyankpatel/ddrive-storage/Lando/test1.png";
                logger.info("File Path: " + filePath);

                Metadata tempMetadata = this.metadataDao.getMetadata(pathParts[1], user.get_id());
                if (tempMetadata == null) {
                    logger.error("Metadata not found");
                    out.writeObject(new Response(StatusCode.NOT_FOUND, "Metadata not found"));
                    return;
                }

                out.writeObject(Response.builder()
                        .statusCode(StatusCode.SUCCESS)
                        .payload(tempMetadata)
                        .build());
                out.flush();

                Response response = (Response) in.readObject();
                if (response.getStatusCode().equals(StatusCode.INTERNAL_SERVER_ERROR)) {
                    logger.info("Not a Success Respoonse from Peer Server on data copy");
                    return;
                }

                File file = fullPath.toFile();
                logger.info("FIle path from peer: " + filePath);
                logger.info("FIle path from peer: " + file.getPath());
                if (!file.exists()) {
                    logger.info("Into File not Exist");
                    out.writeObject(new Response(StatusCode.NOT_FOUND));
                    out.flush();
                    return;
                }
                if (file.exists()) {
                    out.writeObject(new Response(StatusCode.SUCCESS));
                    out.flush();
                    logger.info("INto FIle exist");
                    // int chunkSize = 4096;
                    byte[] buffer = new byte[4096];

                    // Now send the file data in chunks

                    try (FileInputStream fis = new FileInputStream(file)) {
                        int bytesRead;
                        int a = 0;
                        while ((bytesRead = fis.read(buffer)) != -1) {
                            Response chunkresponse = Response.builder()
                                    .statusCode(StatusCode.SUCCESS)
                                    // .payload(fileName)
                                    .data(Arrays.copyOf(buffer, bytesRead)) // Send only the valid bytes
                                    .dataSize(bytesRead)
                                    .build();
                            out.writeObject(chunkresponse);
                            out.flush();
                            // logger.info(a++);
                            // logger.debug("Sending Chunk of size: " + bytesRead);
                            // Thread.sleep(10); // Add a small delay

                        }
                        fis.close();
                        // Signal the end of the file transmission
                        response = Response.builder()
                                .statusCode(StatusCode.EOF)
                                .build();
                        out.writeObject(response);
                        out.flush();

                    } catch (IOException e) {
                        e.printStackTrace();
                        response = new Response(StatusCode.INTERNAL_SERVER_ERROR, "Failed to read file");
                        out.writeObject(response);
                        out.flush();
                    }
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
            return;
        }

        private void handleGetMetadata(Request request) throws IOException {
            String fileName = (String) request.getPayload(); // assuming file name is in payload
            File file = new File(STORAGE_DIRECTORY + "/" + this.currentUser.getUsername() + "/" + fileName);

            // BUG: use metadataDao to get metadata insted of file
            if (file.exists()) {
                String metadata = "File name: " + fileName + "\nSize: " + file.length() + " bytes";
                Response response = new Response(StatusCode.SUCCESS, metadata);
                out.writeObject(response);
            } else {
                Response response = new Response(StatusCode.NOT_FOUND, "File not found");
                out.writeObject(response);
            }
            out.flush();
        }

        private boolean pushReplicationDataToRegistory(ReplicateRequest pushReplication, RequestType requestType) {
            try (Socket registorySocket = new Socket(REGISTRY_HOST, REGISTRY_PORT);
                    ObjectOutputStream out = new ObjectOutputStream(registorySocket.getOutputStream());
                    ObjectInputStream in = new ObjectInputStream(registorySocket.getInputStream());) {

                out.writeObject(PeerRequest.builder()
                        .requestType(requestType)
                        .socketAddress(new InetSocketAddress("localhost", serverSocket.getLocalPort()))
                        .nodeType(NodeType.STORAGE_NODE)
                        .payload(pushReplication)
                        .build());
                out.flush();

                Response response = (Response) in.readObject();
                out.writeObject(PeerRequest.builder()
                        .requestType(RequestType.DISCONNECT)
                        .build());
                out.flush();

                if (response.getStatusCode() == StatusCode.SUCCESS) {

                    logger.info("Data replication request sent to Registory");
                    return true;
                } else {
                    return false;
                }
            } catch (Exception e) {
                return false;
            }
        }

        private InetSocketAddress getLoadBalancerAddress() {
            try (Socket socket = new Socket(REGISTRY_HOST, REGISTRY_PORT);
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

                // Send a request to forward to the correct storage node
                PeerRequest forwardRequest = PeerRequest.builder()
                        .requestType(RequestType.FORWARD_REQUEST)
                        .build();

                out.writeObject(forwardRequest);
                out.flush();
                logger.info("Request sent to Registory");

                Response response = (Response) in.readObject();
                out.writeObject(new Request(RequestType.DISCONNECT));
                out.flush();

                logger.info("Response received from Registory " + response.getStatusCode());
                if (response.getStatusCode() == StatusCode.SUCCESS) {
                    logger.info("LB Address receved from Registory");
                    return (InetSocketAddress) response.getPayload();
                } else {
                    logger.warn("Failed to get load balancer address");
                    return null;
                }
            } catch (IOException | ClassNotFoundException e) {
                logger.error("Failed to get load balancer address");
                return null;
            }
        }
    }
}