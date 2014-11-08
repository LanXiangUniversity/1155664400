package lxu.lxdfs.datanode;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import lxu.lxdfs.Block;
import lxu.lxdfs.namenode.ClientPacket;

/**
 * Created by magl on 14/11/8.
 */
public class BlockService implements Runnable {
    // blockID -> local file name
    private ConcurrentHashMap<Long, String> blockFiles = null;
    private ServerSocket serverSocket = null;

    public BlockService(ServerSocket serverSocket) {
        this.blockFiles = new ConcurrentHashMap<Long, String>();
        this.serverSocket = serverSocket;
    }

    private class BlockReader implements Runnable {
        private ClientPacket packet = null;
        private Socket socket = null;

        public BlockReader(ClientPacket packet, Socket socket) {
            this.packet = packet;
            this.socket = socket;
        }

        @Override
        public void run() {
            Block block = packet.getBlock();
            int ackPacketID = packet.getPacketID();
            String fileName = blockFiles.get(block.getBlockID());
            boolean operationState = false;
            ArrayList<String> lines = new ArrayList<String>();

            if (fileName == null) {
                System.err.println("Error: Data Node received wrong block ID");
            } else {
                try {
                    BufferedReader reader = new BufferedReader(new FileReader(fileName));
                    String line = null;
                    while ((line = reader.readLine()) != null) {
                        lines.add(line);
                    }
                    operationState = true;
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    System.err.println("Error: Data Node reading file " + fileName + " error");
                }
            }
            try {
                ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
                output.writeObject(new DataNodePacket(ackPacketID, block, operationState, lines));
                output.close();
            } catch (IOException e) {
                System.err.println("Error: DataNode writing packet to client error");
                System.err.println(e.getMessage());
            }
        }
    }

    private class BlockWriter implements Runnable {

        private ClientPacket packet = null;
        private Socket socket = null;

        public BlockWriter(ClientPacket packet, Socket socket) {
            this.packet = packet;
            this.socket = socket;
        }

        @Override
        public void run() {
            Block block = packet.getBlock();
            int ackPacketID = packet.getPacketID();
            boolean operationState = false;
            ArrayList<String> lines = packet.getLines();
            String fileName = "blk_" + block.getBlockID();

            try {
                // save block content
                PrintWriter writer = new PrintWriter(fileName);
                for (String line : lines) {
                    writer.println(line);
                }
                operationState = true;
                // record blockID -> fileName
                blockFiles.put(block.getBlockID(), fileName);
            } catch (IOException e) {
                System.err.println("Error: Data Node writing file " + fileName + " error");
            }
            // send ack to client
            try {
                ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
                output.writeObject(new DataNodePacket(ackPacketID, block, operationState, null));
                output.close();
            } catch (IOException e) {
                System.err.println("Error: DataNode writing packet to client error");
                System.err.println(e.getMessage());
            }
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                Socket socket = serverSocket.accept();
                ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
                ClientPacket packet = (ClientPacket)input.readObject();
                switch (packet.getOperation()) {
                    case ClientPacket.BLOCK_READ:
                        (new Thread(new BlockReader(packet, socket))).start();
                        break;
                    case ClientPacket.BLOCK_WRITE:
                        (new Thread(new BlockWriter(packet, socket))).start();
                        break;
                }
            } catch (IOException e) {
                // serverSocket.accept
                System.err.println("Error: DataNode accepting connection error!");
                System.err.println(e.getMessage());
            } catch (ClassNotFoundException e) {
                // readObject
                System.err.println("Error: DataNode received wrong packet type!");
                System.err.println(e.getMessage());
            }
        }
    }
}
