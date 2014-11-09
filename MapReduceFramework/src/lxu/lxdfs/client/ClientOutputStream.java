package lxu.lxdfs.client;

import lxu.lxdfs.datanode.DataNodePacket;
import lxu.lxdfs.metadata.AllocatedBlock;
import lxu.lxdfs.metadata.Block;
import lxu.lxdfs.metadata.DataNodeDescriptor;
import lxu.lxdfs.service.NameSystemService;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.RemoteException;
import java.util.*;

/**
 * Created by Wei on 11/3/14.
 */
public class ClientOutputStream {
	private String fileName;
	private int blockOffset;
	private int listenPort;
	private NameSystemService nameSystem;
	private int blockSize = 10;
	// Locations for all replicas
	private List<DataNodeDescriptor> locations;
	// Packets to be sent.
	private LinkedList<ClientPacket> dataQueue;
	// Packets to be acked.
	private LinkedList<ClientPacket> ackQueue;
	private Queue<String> buffer;
	private LinkedList<AckListener> ackListeners;

	public ClientOutputStream() {
		this.listenPort = 15998;
        locations = new LinkedList<DataNodeDescriptor>();
        dataQueue = new LinkedList<ClientPacket>();
        ackQueue = new LinkedList<ClientPacket>();
		this.ackListeners = new LinkedList<AckListener>();
	}

	public void close() {
        for (AckListener ackListener : ackListeners) {
            if (ackListener.isRunning()) {
                ackListener.stop();
            }
        }
	}

	public int getListenPort() {
		return listenPort;
	}

	public void setListenPort(int listenPort) {
		this.listenPort = listenPort;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public int getBlockOffset() {
		return blockOffset;
	}

	public void setBlockOffset(int blockOffset) {
		this.blockOffset = blockOffset;
	}

	public NameSystemService getNameSystem() {
		return nameSystem;
	}

	public void setNameSystem(NameSystemService nameSystem) {
		this.nameSystem = nameSystem;
	}

	public int getBlockSize() {
		return blockSize;
	}

	public void setBlockSize(int blockSize) {
		this.blockSize = blockSize;
	}

	public List<DataNodeDescriptor> getLocations() {
		return locations;
	}

	public void setLocations(List<DataNodeDescriptor> locations) {
		this.locations = locations;
	}

	public LinkedList<ClientPacket> getDataQueue() {
		return dataQueue;
	}

	public void setDataQueue(LinkedList<ClientPacket> dataQueue) {
		this.dataQueue = dataQueue;
	}

	public LinkedList<ClientPacket> getAckQueue() {
		return ackQueue;
	}

	public void setAckQueue(LinkedList<ClientPacket> ackQueue) {
		this.ackQueue = ackQueue;
	}

	public Queue<String> getBuffer() {
		return buffer;
	}

	public void setBuffer(Queue<String> buffer) {
		this.buffer = buffer;
	}

	/**
	 * Write data to the first DataNode.
	 * Store data in the buffer, and send to the DataNode
	 * if the buffer size >= Block size.
	 *
	 * @param data
	 * @return
	 */
	public int write(String data) {
		int writeSize = 0;
		AllocatedBlock allocatedBlock = null;

		// get data
		String[] lines = data.split("\n");

		// Buffer
		for (String line : lines) {
			buffer.add(line);
		}

		while (buffer.size() >= blockSize) {
			writeSize += buffer.size() < 10 ? buffer.size() : 10;

			// Allocate new Blocks through RPC and get the locations.
			try {
				allocatedBlock = nameSystem.allocateBlock(this.fileName, this.blockOffset);
			} catch (RemoteException e) {
				e.printStackTrace();
			}

			Block block = allocatedBlock.getBlock();
			HashSet<DataNodeDescriptor> locations = allocatedBlock.getLocations();

			// Update info about the first Data Node.

			// Create packet.
			List<ClientPacket> packets = this.getPacketsFromBuffer(locations, block);

			for (ClientPacket packet : packets) {
				// Send packet to the first Data Node.
				this.dataQueue.add(packet);
				this.sendPacket(packet);

				// Wait for ack of this packet.
				this.dataQueue.remove();
				this.ackQueue.add(packet);
			}

			this.blockOffset++;
		}

		/* TODO wait for ackQueue to be cleared by ackListener */

		return writeSize;
	}

	/**
	 * Send packet (Block) to the first Data Node.
	 */
	public void sendPacket(ClientPacket packet) {
	    /*
		String ip = packet.getLocations().get(0).getDataNodeIP();
		int port = packet.getLocations().get(0).getDataNodePort();
		*/
		String ip = packet.getLocation().getDataNodeIP();
		int port = packet.getLocation().getDataNodePort();

		try {
			Socket sock = new Socket(ip, port);
			ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
			oos.writeObject(packet);
            AckListener ackListener = new AckListener(sock);
            ackListeners.add(ackListener);
            (new Thread(ackListener)).start();

			// Log
			System.out.println("Succeed to write to DataNode " +
					packet.getLocation().getDataNodeID());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Get data from buffer and build a packet.
	 *
	 * @return
	 */
	public List<ClientPacket> getPacketsFromBuffer(Set<DataNodeDescriptor> locations,
	                                               Block block) {
		if (buffer.size() == 0) {
			return null;
		}

		List<ClientPacket> packets = new ArrayList<ClientPacket>();

		ArrayList<String> lines = new ArrayList<String>();

		// Get top elements in the buffer.
		int blockLen = blockSize > buffer.size() ? buffer.size() : blockSize;

		while (blockLen-- > 0) {
			lines.add(buffer.remove());
		}

        block.setLen(blockLen);
		// Create a new packet.
		for (DataNodeDescriptor location : locations) {
			ClientPacket packet = new ClientPacket();
			packet.setLines(lines);
			packet.setBlock(block);

			//packet.setLocations(locations);
			packet.setLocation(location);
			packet.setReplicaID(1);
			packet.setReplicaNum(2);
			packets.add(packet);
		}

		return packets;
	}

	/**
	 * Listen for acks from data node.
	 */
	private class AckListener implements Runnable {
        private Socket socket;
		private boolean isRunning = true;

        public AckListener(Socket socket) {
            this.socket = socket;
        }

		public boolean isRunning() {
			return isRunning;
		}

		public void setRunning(boolean isRunning) {
			this.isRunning = isRunning;
		}

		public void stop() {
			this.isRunning = false;
		}

		@Override
		public void run() {
			ObjectInputStream dis = null;
			DataNodePacket packet = null;

			while (this.isRunning) {
				try {
					dis = new ObjectInputStream(socket.getInputStream());
					packet = (DataNodePacket) dis.readObject();

					int ackID = packet.getAckPacketID();
					System.out.println("ACK ID: " + ackID);

                    for (ClientPacket clientPacket : ackQueue) {
                        if (clientPacket.getPacketID() == ackID) {
                            ackQueue.remove(clientPacket);
                            this.isRunning = false;
                            break;
                        }
                    }
				} catch (IOException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
            try {
                socket.close();
                ackListeners.remove(this);
            } catch (IOException e) {
                System.err.println("Error: AckListener close socket wrong");
            }

		}
	}
}
