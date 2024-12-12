package com.cloude;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Peer {

    private ServerSocket peerSocket;

    public Peer(ServerSocket peerSocket) {
        this.peerSocket = peerSocket;
    }

    public void start() {
        try {
            Socket socket = this.peerSocket.accept();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private class PeerHandler implements Runnable {

        private Socket peerSocket;
        private ObjectInputStream in;
        private ObjectOutputStream out;

        public PeerHandler(Socket peerSocket) {
            this.peerSocket = peerSocket;
            try {
                this.in = new ObjectInputStream(this.peerSocket.getInputStream());
                this.out = new ObjectOutputStream(this.peerSocket.getOutputStream());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'run'");
        }

    }

}
