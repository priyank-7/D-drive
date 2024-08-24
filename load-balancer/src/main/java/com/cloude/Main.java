package com.cloude;

public class Main {
    public static void main(String[] args) {
        LoadBalancer loadBalancer = new LoadBalancer(8080);
        loadBalancer.start();
    }
}