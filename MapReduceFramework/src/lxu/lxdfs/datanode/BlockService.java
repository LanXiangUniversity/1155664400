package lxu.lxdfs.datanode;

import lxu.lxdfs.client.ClientPacket;
import lxu.lxdfs.metadata.Block;
import lxu.lxdfs.metadata.DataNodeDescriptor;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BlockService.java
 * <p/>
 * BlockService class helps DataNode to manage all blocks. It maintains a
 * crucial map: block -> local file name.
 * <p/>
 * Inner class {@link BlockReader} is a thread in response to client read
 * request. It will read all contents of a block according to blockID and
 * return contents to client.
 * <p/>
 * Inner class {@link BlockReader} is a thread in response to client write
 * request. It will write all received data to a local file.
 */
public class BlockService implements Runnable {
    // blockID -> local file name
    private static ConcurrentHashMap<Block, String> blockFiles =
            new ConcurrentHashMap<Block, String>();
    private ServerSocket serverSocket = null;
    private boolean isRunning = false;
    private static int datanodeId;

    public BlockService(ServerSocket serverSocket, boolean isRunning) {
        this.serverSocket = serverSocket;
        this.isRunning = isRunning;
    }

    public void stop() {
        this.isRunning = false;
    }

    public static int getDatanodeId() {
        return datanodeId;
    }

    public static void setDatanodeId(int datanodeId) {
        BlockService.datanodeId = datanodeId;
    }

    /**
     * deleteBlock
     *
     * Delete the local file of the block
     *
     * @param block block to be deleted
     */
    public void deleteBlock(Block block) {
        blockFiles.remove(block);
        File localFile = new File("datanode_" + this.datanodeId +
                "/blk_" + block.getBlockID());
        if (!localFile.delete()) {
            System.err.println("Delete operation is failed.");
        }
    }

    /**
     * copyBlockFromAnotherDataNode
     *
     * This function handles {@link lxu.lxdfs.datanode.DataNode} failure situation.
     * When one DataNode fails, {@link lxu.lxdfs.namenode.NameNode} will ask the
     * DataNode to fetch a new block from another DataNode.
     *
     * @param block Block to be fetched
     * @param dataNode Where the block is stored
     */
    public void copyBlockFromAnotherDataNode(Block block, DataNodeDescriptor dataNode) {
        ClientPacket clientPacket = new ClientPacket();
        clientPacket.setBlock(block);
        clientPacket.setOperation(ClientPacket.BLOCK_READ);

        String dataNodeIP = dataNode.getDataNodeIP();
        int dataNodePort = dataNode.getDataNodePort();

        try {
            Socket socket = new Socket(dataNodeIP, dataNodePort);
            ObjectOutputStream outputStream =
                    new ObjectOutputStream(socket.getOutputStream());
            outputStream.writeObject(clientPacket);

            ObjectInputStream inputStream =
                    new ObjectInputStream(socket.getInputStream());
            DataNodePacket receivedPacket =
                    (DataNodePacket) inputStream.readObject();

            List<String> lines = receivedPacket.getLines();
            BlockWriter.writeToFile(block, lines);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        while (isRunning) {
            try {
                Socket socket = serverSocket.accept();
                ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
                ClientPacket packet = (ClientPacket) input.readObject();
                /**
                 * When received a READ request, new a {@link lxu.lxdfs.datanode.BlockService.BlockReader}
                 * to process. If it is a WRITE request, new a {@link lxu.lxdfs.datanode.BlockService.BlockWriter}
                 * to process.
                 */
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
        try {
            this.serverSocket.close();
        } catch (IOException e) {
            System.err.println("DataNode stopping block service error");
            e.printStackTrace();
        }
    }

    public ArrayList<Block> getAllBlocks() {
        return new ArrayList<Block>(blockFiles.keySet());
    }

    /**
     * Thread to handle BLOCK_READ request.
     */
    private static class BlockReader implements Runnable {
        private ClientPacket packet = null;
        private Socket socket = null;

        public BlockReader(ClientPacket packet, Socket socket) {
            this.packet = packet;
            this.socket = socket;
        }

        @Override
        public void run() {
            /*
             * Extract Block and read contents of that block.
             */
            Block block = packet.getBlock();
            int ackPacketID = packet.getPacketID();
            String fileName = blockFiles.get(block);
            boolean operationState = false;
            ArrayList<String> lines = new ArrayList<String>();

            if (fileName == null) {
                System.err.println("Error: Data Node received wrong block ID");
            } else {
                try {
                    BufferedReader reader = new BufferedReader(new FileReader(fileName));
                    String line;
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
            /*
             * Send contents back.
             */
            try {
                ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
                output.writeObject(new DataNodePacket(ackPacketID, block, operationState, lines));
                output.close();
            } catch (IOException e) {
                System.err.println("Error: DataNode writing packet to client error");
                System.err.println(e.getMessage());
            }
            System.out.println("Read block " + block.getBlockID() + "from file " + fileName);
        }
    }

    /**
     * A thread to handle BLOCK_WRITE request.
     */
    private static class BlockWriter implements Runnable {

        private ClientPacket packet = null;
        private Socket socket = null;

        public BlockWriter(ClientPacket packet, Socket socket) {
            this.packet = packet;
            this.socket = socket;
        }

        /**
         * writeToFile
         *
         * Write a list of lines to file.
         *
         * @param block
         * @param lines
         * @return
         */
        public static boolean writeToFile(Block block, List<String> lines) {
            String fileName = "datanode_" + datanodeId + "/blk_" + block.getBlockID();
            File folder = new File("datanode_" + datanodeId);
            if (!folder.exists()) {
                folder.mkdir();
            }
            boolean success = false;

            try {
                File file = new File(fileName);
                if (file.exists()) {
                    file.createNewFile();
                }
                // save block content
                PrintWriter writer = new PrintWriter(fileName);
                for (String line : lines) {
                    writer.println(line);
                }
                // record blockID -> fileName
                blockFiles.put(block, fileName);
                writer.close();
                success = true;
            } catch (IOException e) {
                System.err.println("Error: Data Node writing file " + fileName + " error");
                e.printStackTrace();
            }
            return success;
        }

        @Override
        public void run() {
            Block block = packet.getBlock();
            int ackPacketID = packet.getPacketID();
            boolean operationState = false;
            ArrayList<String> lines = packet.getLines();

            // write contents to file
            operationState = writeToFile(block, lines);

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
}
