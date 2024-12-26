package com.cloude;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {

        if (args.length < 1) {
            System.err.println("Usage: java Main <Registry_Port>");
            System.exit(1);
        }

        int registoryPort = Integer.parseInt(args[0]);
        Registory registory = new Registory(registoryPort);
        registory.start();
    }
}
