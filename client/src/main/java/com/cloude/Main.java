package com.cloude;

public class Main {
    public static void main(String[] args) {
        // Logger logger = LoggerFactory.getLogger(Main.class);
        // logger.info("Hello World");

        Client client = new Client("10.1.48.180", 8080);
        client.HandelRequest();
    }
}