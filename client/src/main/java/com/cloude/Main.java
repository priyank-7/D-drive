package com.cloude;

public class Main {
    public static void main(String[] args) {
        Client client = new Client("localhost", 8080);
        client.HandelRequest();
    }
}