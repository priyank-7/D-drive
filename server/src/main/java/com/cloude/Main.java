package com.cloude;

public class Main {
    public static void main(String[] args) {
        StorageNode storageNode = new StorageNode(9092);
        storageNode.start();
    }
}