package com.cloude;

public class Main {
    public static void main(String[] args) {
        Client client = new Client("localhost", 8080);
        System.out.println("[Client]: Connecting to Load Balancer");
        client.authenticate("user", "password");
        // /Users/priyankpatel/Desktop/test.png
        System.out.println("[Client]: Uploading file");
        client.uploadFile("/Users/priyankpatel/Desktop/test.png");
    }
}