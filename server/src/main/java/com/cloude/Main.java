package com.cloude;

public class Main {
    public static void main(String[] args) {

        if (args.length < 3) {
            System.err.println("Usage: java Main <Registry_IP> <Registry_Port> <StorageNode_Port>");
            System.exit(1);
        }

        String registryIP = args[0];
        int registryPort = Integer.parseInt(args[1]);
        int storagePort = Integer.parseInt(args[2]);

        StorageNode storageNode = new StorageNode(storagePort, registryIP, registryPort);
        storageNode.start();
    }
}