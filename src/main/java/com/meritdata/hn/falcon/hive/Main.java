package com.meritdata.hn.falcon.hive;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.net.InetSocketAddress;

public class Main {
    public static void main(String[] args) throws IOException {
        System.out.println("Hello world!");
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        InetSocketAddress address = new InetSocketAddress("hadoov", 50070);
        serverSocketChannel.bind(address);
    }
}