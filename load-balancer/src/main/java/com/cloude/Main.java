package com.cloude;

public class Main {
    public static void main(String[] args) {
        LoadBalancer loadBalancer = new LoadBalancer(8080);
        loadBalancer.start();
        // UserDAO userDAO = new UserDAO(MongoDBConnection.getDatabase("ddrive"));
        // userDAO.insertUser(User.builder()
        // .username("priyank")
        // .passwordHash("password")
        // .build());
    }
}