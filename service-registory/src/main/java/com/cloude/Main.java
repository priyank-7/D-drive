package com.cloude;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        Registory registory = new Registory(7071);
        registory.start();
    }
}