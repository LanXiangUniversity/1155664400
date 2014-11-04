package lxu.lxdfs.namenode;

import lxu.lxdfs.BlocksLocation;

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
public class ClientOutputStream {
	private String fileName;
	private int blockOffset;
	private NameSystemService nameSystem;
	private int blockSize = 10;

	// Locations for all replicas
	private List<BlocksLocation> locations;

	// Packets to be sent.
	private Queue<ClientPacket> dataQueue;
	// Packets to be acked.
	private Queue<ClientPacket> ackQueue;

	private Queue<String> buffer;

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


	public List<BlocksLocation> getLocations() {
		return locations;
	}

	public void setLocations(List<BlocksLocation> locations) {
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
	public int write(String data) {
		// get data
		String[] lines = data.split("\n");

		// Buffer
		for (String line : lines) {
			buffer.add(line);
		}

		while (buffer.size() >= blockSize) {
			// Allocate new Blocks through RPC and get the locations.
			try {
				List<BlocksLocation> locations = nameSystem.allocateBlock(this.fileName, this.blockOffset);
			} catch (RemoteException e) {
				e.printStackTrace();
			}

			// Update info about the first Data Node.

			// Create packet.
			ClientPacket packet = this.getPacketFromBuffer();

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
		String ip = packet.getLocations().get(0).getDataNode().getDataNodeIP();
		int port = packet.getLocations().get(0).getDataNode().getDataNodePort();

		try {
			Socket sock = new Socket(ip, port);
			ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
			oos.writeObject(packet);
			oos.close();
			sock.close();

			// Log
			System.out.println("Succeed to write to DataNode " +
					packet.getLocations().get(0).getDataNode().getDataNodeID());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Get data from buffer and build a packet.
	 *
	 * @return
	 */
	public ClientPacket getPacketFromBuffer() {
		if (buffer.size() == 0) {
			return null;
		}

		List<String> lines = new ArrayList<String>();

		// Get top elements in the buffer.
		int blockLen = blockSize > buffer.size() ? buffer.size() : blockSize;

		while (blockLen-- > 0) {
			lines.add(buffer.remove());
		}

		// Create a new packet.
		ClientPacket packet = new ClientPacket();
		packet.setLines(lines);
		packet.setLen(buffer.size());
		packet.setLocations(this.locations);
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
