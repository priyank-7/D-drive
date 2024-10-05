package com.cloude;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.logging.log4j.core.LoggerContext;

import java.util.Arrays;
import java.util.Date;

import com.cloude.db.MetadataDao;
import com.cloude.db.User;
import com.cloude.headers.Metadata;
import com.cloude.headers.Request;
import com.cloude.headers.RequestType;
import com.cloude.headers.Response;
import com.cloude.headers.StatusCode;
import com.cloude.utilities.NodeType;
import com.cloude.utilities.PeerRequest;

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

    private static final String LOAD_BALANCER_HOST = "localhost";
    private static final int LOAD_BALANCER_PORT = 8080;
    private static final String REGISTRY_HOST = "localhost"; // Registry service host
    private static final int REGISTRY_PORT = 7070; // Registry service port
    private static final String STORAGE_DIRECTORY = System.getProperty("user.home") + "/ddrive-storage";
    private long lastPingTime;
    private static final long PING_TIMEOUT = 15000; // 15 seconds

    private final ExecutorService threadPool;
    private MetadataDao metadataDao;
    private final BlockingQueue<String> replicationQueue;

    public StorageNode(int port) {

        this.logger.setLevel(org.apache.logging.log4j.Level.INFO);
        this.threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        this.replicationQueue = new LinkedBlockingQueue<>();
        try {
            serverSocket = new ServerSocket(port);
            int poolSize = Runtime.getRuntime().availableProcessors();
            metadataDao = new MetadataDao("ddrive", "metadata");
            logger.info("Thread pool size: " + poolSize);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (registerWithRegistry()) {
            logger.info("Successfully registered with the Registry");
        } else {

            logger.error("Failed to register with the Registry");
        }
    }

    public void start() {
        logger.info("Listening on port " + serverSocket.getLocalPort());
        while (true) {
            try {
                Socket clientSocket = serverSocket.accept();
                threadPool.submit(new ClientHandler(clientSocket, this.metadataDao));
                logger.info("Accepted connection from " + clientSocket.getPort());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean registerWithRegistry() {
        try (Socket registrySocket = new Socket(REGISTRY_HOST, REGISTRY_PORT);
                ObjectOutputStream out = new ObjectOutputStream(registrySocket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(registrySocket.getInputStream())) {

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

                    if (request.getRequestType() == RequestType.PING) {
                        handlePingRequest();
                        clientSocket.close();
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
                logger.info("Closing connection");
                try {
                    if (!clientSocket.isClosed()) {
                        clientSocket.close(); // Ensure the socket is closed in case of exceptions
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        private void handlePingRequest() {
            try {
                // Send a simple PONG response back to the client
                out.writeObject(new Response(StatusCode.PONG, "PONG"));
                out.flush();
            } catch (IOException e) {
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
            logger.info("uploade file request receved");
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

                // this.replicationQueue.put(tempMetadata.getPath());
                // TODO: Spin up a new thread to send data to registory

                replicationQueue.add(tempMetadata.getPath());
                pushReplicationDataToRegistory(this.currentUser.getUsername() + "/" + metadata.getName());
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
            File file = new File(tempMetadata.getPath());

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
                        response = Response.builder()
                                .statusCode(StatusCode.SUCCESS)
                                .payload(fileName)
                                .data(Arrays.copyOf(buffer, bytesRead)) // Send only the valid bytes
                                .dataSize(bytesRead)
                                .build();
                        out.writeObject(response);
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
            System.out.println("metadata: " + tempMetaData);
            if (tempMetaData == null) {
                logger.warn("File not found");
                Response response = new Response(StatusCode.NOT_FOUND, "File not found");
                out.writeObject(response);
                out.flush();
                return;
            }
            if (!this.metadataDao.deleteMetadata(tempMetaData)) {
                logger.warn("Failed to delete metadata");
                Response response = new Response(StatusCode.INTERNAL_SERVER_ERROR, "Failed to delete metadata");
                out.writeObject(response);
                out.flush();
                return;
            }

            // TODO: Add info into replication queue and update it with central server

            File file = new File(tempMetaData.getPath());
            if (file.exists()) {
                if (file.delete()) {
                    Response response = new Response(StatusCode.SUCCESS, "File deleted successfully");
                    out.writeObject(response);
                } else {
                    Response response = new Response(StatusCode.INTERNAL_SERVER_ERROR, "Failed to delete file");
                    out.writeObject(response);
                }
            } else {
                Response response = new Response(StatusCode.NOT_FOUND, "File not found");
                out.writeObject(response);
            }
            out.flush();
        }

        private void handleGetMetadata(Request request) throws IOException {
            String fileName = (String) request.getPayload(); // assuming file name is in payload
            File file = new File(STORAGE_DIRECTORY + fileName);
            if (file.exists()) {
                String metadata = "File name: " + fileName + "\nSize: " + file.length() + " bytes";
                Response response = new Response(StatusCode.SUCCESS, metadata);
                out.writeObject(response);
            } else {
                Response response = new Response(StatusCode.NOT_FOUND, "File not found");
                out.writeObject(response);
            }
        }

        private boolean pushReplicationDataToRegistory(String path) {
            try (Socket registorySocket = new Socket(REGISTRY_HOST, REGISTRY_PORT);
                    ObjectOutputStream out = new ObjectOutputStream(registorySocket.getOutputStream());
                    ObjectInputStream in = new ObjectInputStream(registorySocket.getInputStream());) {

                out.writeObject(PeerRequest.builder()
                        .requestType(RequestType.PUSH_DATA)
                        .socketAddress(new InetSocketAddress("localhost", serverSocket.getLocalPort()))
                        .payload(path));
                out.flush();
                Response response = (Response) in.readObject();
                out.writeObject(PeerRequest.builder()
                        .requestType(RequestType.DISCONNECT));
                out.flush();
                if (response.getStatusCode() == StatusCode.SUCCESS) {
                    return true;
                } else {
                    return false;
                }
            } catch (Exception e) {
                return false;
            }
        }
    }
}
