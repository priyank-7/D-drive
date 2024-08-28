package com.cloude;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Arrays;
import com.cloude.headers.Metadata;
import com.cloude.headers.Request;
import com.cloude.headers.RequestType;
import com.cloude.headers.Response;
import com.cloude.headers.StatusCode;

public class StorageNode {
    private ServerSocket serverSocket;
    private static final String LOAD_BALANCER_HOST = "localhost";
    private static final int LOAD_BALANCER_PORT = 8080;
    private final ExecutorService threadPool;
    private static final String STORAGE_DIRECTORY = "/Users/priyankpatel/Documents/storage/";

    public StorageNode(int port) {
        ExecutorService tempThreadPool = null;
        try {
            serverSocket = new ServerSocket(port);
            int poolSize = Runtime.getRuntime().availableProcessors();
            System.out.println("Pool size: " + poolSize);
            tempThreadPool = Executors.newFixedThreadPool(poolSize);
        } catch (IOException e) {
            e.printStackTrace();
            tempThreadPool = Executors.newFixedThreadPool(1); // Default to a single-thread pool in case of error
        } finally {
            this.threadPool = tempThreadPool;
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
                    String token = request.getToken();

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
                try {
                    if (!clientSocket.isClosed()) {
                        clientSocket.close(); // Ensure the socket is closed in case of exceptions
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
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
                return response.getStatusCode() == StatusCode.SUCCESS;

            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
                return false;
            }
        }

        // Handle Upload File Request

        private void handleUploadFile(Request request) throws IOException {
            System.out.println("[Storage Node]: file metadata received");
            Metadata metadata = (Metadata) request.getPayload();
            String fileName = metadata.getName();
            File file = new File(STORAGE_DIRECTORY + fileName);

            if (file.exists()) {
                out.writeObject(new Response(StatusCode.SUCCESS, "File already exists"));
            } else {
                out.writeObject(new Response(StatusCode.SUCCESS, "Ready to receive file"));
            }
            out.flush();
            System.out.println("[Storage Node]: Ready to receive file");

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
            File file = new File(STORAGE_DIRECTORY + fileName);

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
            File file = new File(STORAGE_DIRECTORY + fileName);

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
