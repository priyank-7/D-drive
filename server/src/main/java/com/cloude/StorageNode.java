package com.cloude;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

public class StorageNode {
    private ServerSocket serverSocket;
    private static final String LOAD_BALANCER_HOST = "localhost";
    private static final int LOAD_BALANCER_PORT = 8080;
    private static final String STORAGE_DIRECTORY = "/Users/priyankpatel/Documents/storage/";

    private static final String REGISTRY_HOST = "localhost"; // Registry service host
    private static final int REGISTRY_PORT = 7070; // Registry service port

    private final ExecutorService threadPool;
    private MetadataDao metadataDao;

    public StorageNode(int port) {
        ExecutorService tempThreadPool = null;
        try {
            serverSocket = new ServerSocket(port);
            int poolSize = Runtime.getRuntime().availableProcessors();
            metadataDao = new MetadataDao("ddrive", "metadata");
            System.out.println("Pool size: " + poolSize);
            tempThreadPool = Executors.newFixedThreadPool(poolSize);
        } catch (IOException e) {
            e.printStackTrace();
            tempThreadPool = Executors.newFixedThreadPool(1); // Default to a single-thread pool in case of error
        } finally {
            this.threadPool = tempThreadPool;
        }

        if (registerWithRegistry()) {
            System.out.println("Successfully registered with the Registry");
        } else {
            System.out.println("Failed to register with the Registry");
        }
    }

    public void start() {
        System.out.println("[Storage Node]: Listening on port " + serverSocket.getLocalPort());
        while (true) {
            try {
                Socket clientSocket = serverSocket.accept();
                threadPool.submit(new ClientHandler(clientSocket, this.metadataDao));
                System.out.println("[Storage Node]: Accepted connection from " + clientSocket.getPort());
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
            // TODO: Get the actual IP address of the storage node
            PeerRequest regiPeerRequest = PeerRequest.builder()
                    .requestType(RequestType.REGISTER)
                    .nodeType(NodeType.STORAGE_NODE)
                    .socketAddress(new InetSocketAddress("localhost", serverSocket.getLocalPort()))
                    .build();
            out.writeObject(regiPeerRequest);
            out.flush();

            Response response = (Response) in.readObject();
            if (response.getStatusCode() == StatusCode.SUCCESS) {
                return true;
            } else {
                System.out.println("Registry registration failed: " + response.getPayload());
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
                    System.out.println("[Storage Node]: Waiting for requests...");
                    Request request = (Request) in.readObject();
                    String token = request.getToken();
                    System.out.println("[Storage Node]: Received request: " + request.getRequestType());

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
                                System.out.println("[Storage Node]: Disconnecting client");
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
                System.out.println("[Storage Node]: Closing connection");
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
                System.out.println("[Storage Node]: Token validation response: " + response);
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
            System.out.println("[Storage Node]: file metadata received");
            Metadata metadata = (Metadata) request.getPayload();
            System.out.println("metadata recieved");
            String filePath = STORAGE_DIRECTORY + File.pathSeparatorChar + this.currentUser.getUsername()
                    + File.pathSeparatorChar + metadata.getName();

            Metadata tempMetadata = this.metadataDao.getMetadata(metadata.getName(), this.currentUser.get_id());
            System.out.println("metadata: " + this.currentUser.get_id());
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
                System.out.println("Metadata: " + tempMetadata);
                if (this.metadataDao.saveMetadata(tempMetadata)) {
                    System.out.println("Metadata saved successfully");
                    out.writeObject(new Response(StatusCode.SUCCESS, "File Created Successfully"));
                    out.flush();
                } else {
                    out.writeObject(new Response(StatusCode.INTERNAL_SERVER_ERROR, "Error to save metadata"));
                    out.flush();
                }
            } else {
                tempMetadata.setModifiedDate(new Date());
                tempMetadata.setSize(metadata.getSize());
                this.metadataDao.updateMetadata(tempMetadata.getName(), this.currentUser.get_id(), tempMetadata);
                out.writeObject(new Response(StatusCode.SUCCESS, "File already exists"));
            }

            System.out.println("[Storage Node]: Ready to receive file");
            File file = new File(filePath);
            // Open the file for writing (append mode)
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
                        System.out.println("[StorageNode]: Client acknowledged metadata receipt.");
                    } else {
                        System.out.println("[StorageNode]: Client failed to acknowledge metadata receipt.");
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
            if (tempMetaData == null) {
                Response response = new Response(StatusCode.NOT_FOUND, "File not found");
                out.writeObject(response);
                out.flush();
                return;
            }
            this.metadataDao.deleteMetadata(tempMetaData);
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
    }
}
