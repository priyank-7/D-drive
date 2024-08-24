package com.cloude;

import java.io.*;
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

    public void connect(String host, int port) throws ClassNotFoundException {
        try (Socket socket = new Socket(host, port);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
            System.out.println("Connected to " + host + ":" + port);

            Request request = Request.builder()
                    .requestType(RequestType.AUTHENTICATE)
                    .payload("Hello from client")
                    .build();
            out.writeObject(request);
            Response response = (Response) in.readObject();
            System.out.println("Response: " + response.getPayload());
            socket.close();

        } catch (IOException e) {
            e.printStackTrace();
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
            System.out.println("Sent authentication request");

            Response response = (Response) in.readObject();
            System.out.println("Received response");

            if (response.getStatusCode() == StatusCode.SUCCESS) {
                token = (String) response.getPayload();
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
                    .payload(filePath)
                    .build();
            forwardRequest.setToken(token);
            out.writeObject(forwardRequest);

            Response response = (Response) in.readObject();

            if (response.getStatusCode() == StatusCode.SUCCESS) {
                String storageNodeAddress = (String) response.getPayload();
                String[] addressParts = storageNodeAddress.split(":");
                String storageNodeHost = addressParts[0];
                int storageNodePort = Integer.parseInt(addressParts[1]);

                // Now connect directly to the storage node to upload the file
                uploadFileToStorageNode(filePath, storageNodeHost, storageNodePort);
            } else {
                System.out.println("Failed to forward request: " + response.getPayload());
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void uploadFileToStorageNode(String filePath, String storageNodeHost, int storageNodePort) {
        try (Socket storageSocket = new Socket(storageNodeHost, storageNodePort);
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
                    .payload(metadata)
                    .build();
            fileUploadRequest.setToken(token);
            out.writeObject(fileUploadRequest);

            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = fileInput.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }

            out.flush();

            // Get the response from the storage node
            Response response = (Response) in.readObject();
            if (response.getStatusCode() == StatusCode.SUCCESS) {
                System.out.println("File uploaded successfully.");
            } else {
                System.out.println("File upload failed: " + response.getPayload());
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // public static void main(String[] args) throws ClassNotFoundException {
    // Client client = new Client("localhost", 8080);
    // if (client.authenticate("user", "password")) {
    // client.uploadFile("path/to/file.txt");
    // }
    // // client.connect("localhost", 9090);
    // }
}
