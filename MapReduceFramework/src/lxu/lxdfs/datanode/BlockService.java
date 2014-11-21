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
 *
 * BlockService class helps DataNode to manage all blocks. It maintains a
 * crucial map: block -> local file name.
 *
 * Inner class {@link BlockReader} is a thread in response to client read
 * request. It will read all contents of a block according to blockID and
 * return contents to client.
 *
 * Inner class {@link BlockReader} is a thread in response to client write
 * request. It will write all received data to a local file.
 */
public class BlockService implements Runnable {
	// blockID -> local file name
	private static ConcurrentHashMap<Block, String> blockFiles =
            new ConcurrentHashMap<Block, String>();
	private ServerSocket serverSocket = null;
    private boolean isRunning = false;

	public BlockService(ServerSocket serverSocket, boolean isRunning) {
		this.serverSocket = serverSocket;
        this.isRunning = isRunning;
	}

    public void stop() {
        this.isRunning = false;
    }

    public void deleteBlock(Block block) {
        blockFiles.remove(block);
        File localFile = new File("blk_" + block.getBlockID());
        if (localFile.delete()) {
            System.out.println(localFile.getName() + " is deleted!");
        } else {
            System.out.println("Delete operation is failed.");
        }
    }

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
                    (DataNodePacket)inputStream.readObject();

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
				//System.out.println(packet.getBlock().getBlockID());
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

	private static class BlockReader implements Runnable {
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

	private static class BlockWriter implements Runnable {

		private ClientPacket packet = null;
		private Socket socket = null;

		public BlockWriter(ClientPacket packet, Socket socket) {
			this.packet = packet;
			this.socket = socket;
		}

        public static boolean writeToFile(Block block, List<String> lines) {
            String fileName = "blk_" + block.getBlockID();
            boolean success = false;

            try {
                // save block content
                PrintWriter writer = new PrintWriter(fileName);
                for (String line : lines) {
                    writer.println(line);
                }
                // record blockID -> fileName
                blockFiles.put(block, fileName);
                writer.close();
                success = true;
                System.out.println("Write block " + block.getBlockID() + " to file " + fileName);
            } catch (IOException e) {
                System.err.println("Error: Data Node writing file " + fileName + " error");
            }
            return success;
        }

		@Override
		public void run() {
			Block block = packet.getBlock();
			int ackPacketID = packet.getPacketID();
			boolean operationState = false;
			ArrayList<String> lines = packet.getLines();

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
