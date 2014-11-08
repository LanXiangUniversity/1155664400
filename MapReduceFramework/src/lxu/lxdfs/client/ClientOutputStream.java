package lxu.lxdfs.client;

import lxu.lxdfs.metadata.BlocksLocation;
import lxu.lxdfs.metadata.DataNodeDescriptor;
import lxu.lxdfs.service.NameSystemService;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * Created by Wei on 11/3/14.
 */
public class ClientOutputStream extends ClientStream {
	private String fileName;
	private int blockOffset;
	private NameSystemService nameSystem;
	private int blockSize = 10;

	// Locations for all replicas
	private ArrayList<BlocksLocation> locations;

	// Packets to be sent.
	private Queue<ClientPacket> dataQueue;
	// Packets to be acked.
	private Queue<ClientPacket> ackQueue;

	// Store lines of data.
	private Queue<String> buffer;

	/**
	 * Getters and Setters.
	 * @return
	 */
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

	public ArrayList<BlocksLocation> getLocations() {
		return locations;
	}

	public void setLocations(ArrayList<BlocksLocation> locations) {
		this.locations = locations;
	}

	public Queue<ClientPacket> getDataQueue() {
		return dataQueue;
	}

	public void setDataQueue(Queue<ClientPacket> dataQueue) {
		this.dataQueue = dataQueue;
	}

	public Queue<ClientPacket> getAckQueue() {
		return ackQueue;
	}

	public void setAckQueue(Queue<ClientPacket> ackQueue) {
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
	public int write(String data) throws RemoteException {
		// Split data into lines.
		String[] lines = data.split("\n");

		// Add lines to the buffer.
		for (String line : lines) {
			buffer.add(line);
		}

		while (buffer.size() >= blockSize) {
			// Allocate new Blocks through RPC and get the locations.
			ArrayList<DataNodeDescriptor> locations = nameSystem.allocateBlock(
															this.fileName, this.blockOffset);

			// Update info about the first Data Node.

			// Create packet.
			ClientPacket packet = this.getPacketFromBuffer(locations);

			// Send packet to the first Data Node.
			this.dataQueue.add(packet);
			this.sendPacket(packet);

			// Wait for ack of this packet.
			this.dataQueue.remove();
			this.ackQueue.add(packet);

			this.blockOffset++;
		}

		return -1;
	}

	/**
	 * Send packet (Block) to the first Data Node.
	 */
	public void sendPacket(ClientPacket packet) {
		String ip = packet.getLocations().get(0).getDataNodeIP();
		int port = packet.getLocations().get(0).getDataNodePort();

		try {
			Socket sock = new Socket(ip, port);
			ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
			oos.writeObject(packet);
			oos.close();
			sock.close();

			// Log
			System.out.println("Succeed to write to DataNode " +
					packet.getLocations().get(0).getDataNodeID());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Get data from buffer and build a packet.
	 * @return
	 */
	public ClientPacket getPacketFromBuffer(ArrayList<DataNodeDescriptor> locations) {
		if (buffer.size() == 0) {
			return null;
		}

		ArrayList<String> lines = new ArrayList<String>();

		// Get top elements in the buffer.
		int blockLen = blockSize > buffer.size() ? buffer.size() : blockSize;

		while (blockLen-- > 0) {
			lines.add(buffer.remove());
		}

		// Create a new packet.
		ClientPacket packet = new ClientPacket();
		packet.setLines(lines);
		packet.setLen(buffer.size());
		packet.setLocations(locations);
		packet.setReplicaID(1);
		packet.setReplicaNum(2);

		return packet;
	}

	/**
	 * Listen for acks from data node.
	 */
	private class AckListener implements Runnable {
		@Override
		public void run() {

		}
	}
}
