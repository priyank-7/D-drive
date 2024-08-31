package com.cloude;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Scanner;

import com.cloude.Commands.UserCommands;
import com.cloude.headers.Metadata;
import com.cloude.headers.Request;
import com.cloude.headers.RequestType;
import com.cloude.headers.Response;
import com.cloude.headers.StatusCode;

public class Client {
    private String loadBalancerHost;
    private int loadBalancerPort;
    private String token;
    private String destinationPath = "/Users/priyankpatel/Downloads";
    Scanner scanner;

    // TODO: Impliment connection timeout
    // TODO: Impliment TCP Blocking Queue for multiple requests

    public Client(String loadBalancerHost, int loadBalancerPort) {
        this.loadBalancerHost = loadBalancerHost;
        this.loadBalancerPort = loadBalancerPort;
        scanner = new Scanner(System.in);
    }

    public void HandelRequest() {
        String input;
        while (true) {
            System.out.print("Enter command: ");
            input = scanner.nextLine().trim();
            String[] parts = input.split(" ", 2);
            String commandStr = parts[0].toUpperCase();
            String argument = parts.length > 1 ? parts[1] : "";

            UserCommands command;
            try {
                command = UserCommands.valueOf(commandStr);
            } catch (IllegalArgumentException e) {
                System.out.println("Unknown command");
                continue;
            }

            switch (command) {
                case AUTH:
                    if (argument.isEmpty()) {
                        System.out.println("Requires username:password formate.");
                    } else {
                        String[] credentials = argument.split(":");
                        if (credentials.length != 2) {
                            System.out.println("Invalid credentials format. Use 'username:password'.");
                        } else {
                            if (authenticate(credentials[0], credentials[1])) {
                                System.out.println("Authenticated successfully.");
                            } else {
                                System.out.println("Authentication failed.");
                            }
                        }
                    }
                    break;

                case PUT:
                    if (argument.isEmpty()) {
                        System.out.println("Requires a file path.");
                    } else {
                        if (token == null) {
                            System.out.println("Please authenticate first.");
                            break;
                        }
                        InetSocketAddress nodeAddr = forwardRequest();
                        if (nodeAddr != null) {
                            uploadFileToStorageNode(argument, nodeAddr);
                        }
                    }
                    break;

                case GET:
                    if (argument.isEmpty()) {
                        System.out.println("GET command requires a file name");
                    } else {
                        if (token == null) {
                            System.out.println("Please authenticate first.");
                            break;
                        }
                        InetSocketAddress nodeAddr = forwardRequest();
                        if (nodeAddr != null) {
                            downloadFileFromStorageNode(argument, nodeAddr);
                        }
                    }
                    break;

                case DELETE:
                    if (argument.isEmpty()) {
                        System.out.println("DELETE command requires a file path.");
                    } else {
                        if (token == null) {
                            System.out.println("Please authenticate first.");
                            break;
                        }
                        InetSocketAddress nodeAddr = forwardRequest();
                        if (nodeAddr != null) {
                            deleteFileFromStorageNode(argument, nodeAddr);
                        }
                    }
                    break;

                case LIST:
                    if (token == null) {
                        System.out.println("Please authenticate first.");
                        break;
                    }
                    InetSocketAddress nodeAddr = forwardRequest();
                    if (nodeAddr != null) {
                        // listFilesInStorageNode(nodeAddr);
                    }
                    break;

                case EXIT:
                    System.out.println("Exiting the client...");
                    return;

                default:
                    System.out.println("Unknown command. Please try again.");
                    break;
            }
        }
    }

    private InetSocketAddress forwardRequest() {
        try (Socket socket = new Socket(loadBalancerHost, loadBalancerPort);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            // Send a request to forward to the correct storage node
            Request forwardRequest = Request.builder()
                    .requestType(RequestType.FORWARD_REQUEST)
                    .token(this.token)
                    .build();
            out.writeObject(forwardRequest);
            out.flush();

            Response response = (Response) in.readObject();
            out.writeObject(new Request(RequestType.DISCONNECT, this.token));
            System.out.println("[client]: Got sotage node ip and port: " + response.getPayload().toString());

            if (response.getStatusCode() == StatusCode.SUCCESS) {
                return (InetSocketAddress) response.getPayload();
            } else {
                System.out.println("Failed to forward request: " + response.getPayload());
                return null;
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    public boolean authenticate(String username, String password) {
        try (Socket socket = new Socket(loadBalancerHost, loadBalancerPort);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            System.out.println("Connected to load balancer");
            String credentials = username + ":" + password;
            Request request = Request.builder()
                    .requestType(RequestType.AUTHENTICATE)
                    .payload(credentials)
                    .build();
            out.writeObject(request);
            out.flush();

            Response response = (Response) in.readObject();
            out.writeObject(new Request(RequestType.DISCONNECT, this.token));

            if (response.getStatusCode() == StatusCode.SUCCESS) {
                this.token = (String) response.getPayload();
                System.out.println("Authenticated successfully. Token: " + token);
                return true;
            } else {
                System.out.println("Authentication failed: " + response.getPayload());
                return false;
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return false;
        }
    }

    private void uploadFileToStorageNode(String filePath, InetSocketAddress storageNodeAddress) {
        try (Socket storageSocket = new Socket(storageNodeAddress.getAddress(), storageNodeAddress.getPort());
                ObjectOutputStream out = new ObjectOutputStream(storageSocket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(storageSocket.getInputStream());
                FileInputStream fileInput = new FileInputStream(filePath)) {

            System.out.println("[Client]: Connected to storage node");
            // Create file metadata to send to the storage node
            File file = new File(filePath);
            Metadata metadata = Metadata.builder()
                    .name(file.getName())
                    .size(file.length())
                    .isFolder(false)
                    .build();

            System.out.println("[Client]: Sending metadata to storage node");
            // Send metadata to the storage node
            Request fileUploadRequest = Request.builder()
                    .requestType(RequestType.UPLOAD_FILE)
                    .token(token)
                    .payload(metadata)
                    .build();
            out.writeObject(fileUploadRequest);
            out.flush();

            // Wait for the storage node to acknowledge metadata receipt
            Response response = (Response) in.readObject();
            if (response.getStatusCode() != StatusCode.SUCCESS) {
                System.out.println("Failed to upload file: " + response.getPayload());
                out.writeObject(new Request(RequestType.DISCONNECT, this.token));
                return;
            }
            System.out.println("[Client]: Metadata received by storage node");
            System.out.println("[Client]: Uploading file data");

            // Send the file data in chunks using Response objects
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = fileInput.read(buffer)) != -1) {
                Response chunkResponse = Response.builder()
                        .statusCode(StatusCode.SUCCESS)
                        .data(Arrays.copyOf(buffer, bytesRead))
                        .dataSize(bytesRead)
                        .build();
                out.writeObject(chunkResponse);
                out.flush();
            }

            // Signal the end of the file upload
            Response endResponse = Response.builder()
                    .statusCode(StatusCode.EOF)
                    .build();
            out.writeObject(endResponse);
            out.flush();

            // Get the final response from the storage node
            response = (Response) in.readObject();
            if (response.getStatusCode() == StatusCode.SUCCESS) {
                System.out.println("File uploaded successfully.");
            } else {
                System.out.println("File upload failed: " + response.getPayload());
            }

            // Send request to disconnect
            out.writeObject(new Request(RequestType.DISCONNECT, this.token));
            out.flush();
            System.out.println("[Client]: Disconnected from storage node");

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void downloadFileFromStorageNode(String fileName, InetSocketAddress storageNodeAddress) {
        try (Socket storageSocket = new Socket(storageNodeAddress.getAddress(), storageNodeAddress.getPort());
                ObjectOutputStream out = new ObjectOutputStream(storageSocket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(storageSocket.getInputStream())) {

            // connectte
            System.out.println("[Client]: Connected to storage node");

            // Send download file request
            Request downloadRequest = Request.builder()
                    .requestType(RequestType.DOWNLOAD_FILE)
                    .token(token)
                    .payload(fileName)
                    .build();
            out.writeObject(downloadRequest);
            out.flush();

            // Receive file metadata
            Response response = (Response) in.readObject();
            if (response.getStatusCode() != StatusCode.SUCCESS) {
                System.out.println("Failed to download file: " + response.getPayload());
                out.writeObject(new Request(RequestType.DISCONNECT, this.token));
                return;
            }

            Metadata metadata = (Metadata) response.getPayload();
            System.out.println("[Client]: Metadata received. File size: " + metadata.getSize() + " bytes");

            // Acknowledge metadata receipt
            Response metadataAckResponse = Response.builder()
                    .statusCode(StatusCode.SUCCESS)
                    .build();
            out.writeObject(metadataAckResponse);
            out.flush();

            // Receive file data in chunks
            try (FileOutputStream fos = new FileOutputStream(destinationPath + File.separator + metadata.getName())) {
                while (true) {
                    response = (Response) in.readObject();

                    if (response.getStatusCode() == StatusCode.EOF) {
                        System.out.println("File download complete.");
                        break;
                    } else if (response.getStatusCode() == StatusCode.SUCCESS) {
                        byte[] data = response.getData();
                        fos.write(data, 0, response.getDataSize());
                    } else {
                        System.out.println("File download failed: " + response.getPayload());
                        break;
                    }
                }
            }

            // Send request to disconnect
            out.writeObject(new Request(RequestType.DISCONNECT, this.token));
            out.flush();
            System.out.println("[Client]: Disconnected from storage node");

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void deleteFileFromStorageNode(String filePath, InetSocketAddress storageNodeAddress) {
        try (Socket storageSocket = new Socket(storageNodeAddress.getAddress(), storageNodeAddress.getPort());
                ObjectOutputStream out = new ObjectOutputStream(storageSocket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(storageSocket.getInputStream())) {

            System.out.println("[Client]: Connected to storage node");

            Request deleteRequest = Request.builder()
                    .requestType(RequestType.DELETE_FILE)
                    .token(token)
                    .payload(filePath)
                    .build();
            out.writeObject(deleteRequest);
            out.flush();

            Response response = (Response) in.readObject();
            if (response.getStatusCode() == StatusCode.SUCCESS) {
                System.out.println("File deleted successfully.");
            } else {
                System.out.println("Failed to delete file: " + response.getPayload());
            }

            out.writeObject(new Request(RequestType.DISCONNECT, this.token));
            out.flush();
            System.out.println("[Client]: Disconnected from storage node");

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

}
