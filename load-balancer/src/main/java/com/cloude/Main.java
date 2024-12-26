package com.cloude;

public class Main {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: java Main <Registry_IP> <Registry_Port> <LoadBalancer_Port>");
            System.exit(1);
        }

        String registryIP = args[0];
        int registryPort = Integer.parseInt(args[1]);
        int loadBalancerPort = Integer.parseInt(args[2]);

        LoadBalancer loadBalancer = new LoadBalancer(loadBalancerPort, registryIP, registryPort);
        loadBalancer.start();
        // UserDAO userDAO = new UserDAO(MongoDBConnection.getDatabase("Ddrive"));
        // System.out.println("UserDAO initialized");
        // userDAO.insertUser(User.builder()
        // .username("lando")
        // .passwordHash("norris")
        // .build());
        // System.out.println("User inserted successfully");
    }
}