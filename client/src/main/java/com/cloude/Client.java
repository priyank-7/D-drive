package com.cloude;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;

import com.cloude.headers.Metadata;
import com.cloude.headers.Request;
import com.cloude.headers.RequestType;
import com.cloude.headers.Response;
import com.cloude.headers.StatusCode;

public class Client {
    private String loadBalancerHost;
    private int loadBalancerPort;
    private String token;

    // TODO: Impliment connection timeout
    // TODO: Impliment TCP Blocking Queue for multiple requests

    public Client(String loadBalancerHost, int loadBalancerPort) {
        this.loadBalancerHost = loadBalancerHost;
        this.loadBalancerPort = loadBalancerPort;
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
            out.writeObject(new Request(RequestType.DISCONNECT));

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

    public void uploadFile(String filePath) {
        if (token == null) {
            System.out.println("Please authenticate first.");
            return;
        }

        try (Socket socket = new Socket(loadBalancerHost, loadBalancerPort);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            // Send a request to forward to the correct storage node
            Request forwardRequest = Request.builder()
                    .requestType(RequestType.FORWARD_REQUEST)
                    .token(this.token)
                    .payload(filePath)
                    .build();
            out.writeObject(forwardRequest);
            out.flush();

            Response response = (Response) in.readObject();
            out.writeObject(new Request(RequestType.DISCONNECT));

            if (response.getStatusCode() == StatusCode.SUCCESS) {
                // Now connect directly to the storage node to upload the file
                uploadFileToStorageNode(filePath, (InetSocketAddress) response.getPayload());
            } else {
                System.out.println("Failed to forward request: " + response.getPayload());
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void uploadFileToStorageNode(String filePath, InetSocketAddress storageNodeAddress) {
        try (Socket storageSocket = new Socket(storageNodeAddress.getAddress(), storageNodeAddress.getPort());
                ObjectOutputStream out = new ObjectOutputStream(storageSocket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(storageSocket.getInputStream());
                FileInputStream fileInput = new FileInputStream(filePath)) {

            // Create file metadata to send to the storage node
            File file = new File(filePath);
            Metadata metadata = Metadata.builder()
                    .name(file.getName())
                    .size(file.length())
                    .isFolder(false)
                    .build();

            // Send metadata and file content
            Request fileUploadRequest = Request.builder()
                    .requestType(RequestType.UPLOAD_FILE)
                    .token(token)
                    .payload(metadata)
                    .build();
            out.writeObject(fileUploadRequest);
            out.flush();

            Response response = (Response) in.readObject();

            // TODO: Handle case based on response of storage node

            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = fileInput.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
            out.flush();

            // Get the response from the storage node
            response = (Response) in.readObject();
            if (response.getStatusCode() == StatusCode.SUCCESS) {
                System.out.println("File uploaded successfully.");
            } else {
                System.out.println("File upload failed: " + response.getPayload());
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
