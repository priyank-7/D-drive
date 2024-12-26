package com.cloude;

public class Main {
    public static void main(String[] args) {
        // Logger logger = LoggerFactory.getLogger(Main.class);
        // logger.info("Hello World");

        if (args.length < 2) {
            System.err.println("Usage: java Main <LoadBalancer_Ip> <LoadBalancer_Port>");
            System.exit(1);
        }

        String LoadBalancer_Ip = args[0];
        int LoadBalancer_Port = Integer.parseInt(args[1]);
        Client client = new Client(LoadBalancer_Ip, LoadBalancer_Port);
        client.HandelRequest();
    }
}