package com.cloude;

public class Main {
    public static void main(String[] args) {
        LoadBalancer loadBalancer = new LoadBalancer(8080);
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