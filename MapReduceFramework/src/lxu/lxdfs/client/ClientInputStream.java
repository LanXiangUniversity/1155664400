package lxu.lxdfs.client;

import lxu.lxdfs.datanode.DataNodePacket;
import lxu.lxdfs.metadata.AllocatedBlock;
import lxu.lxdfs.metadata.Block;
import lxu.lxdfs.metadata.DataNodeDescriptor;
import lxu.lxdfs.service.NameSystemService;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Created by Wei on 11/8/14.
 */
public class ClientInputStream extends ClientStream {
	private String fileName;
	private NameSystemService nameSystemService;

	public ClientInputStream(String fileName) {
		this.fileName = fileName;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public NameSystemService getNameSystemService() {
		return nameSystemService;
	}

	public void setNameSystemService(NameSystemService nameSystemService) {
		this.nameSystemService = nameSystemService;
	}

	// Read the content of file.
	public String read() throws IOException, ClassNotFoundException {
		// Get  AllocatedBlocks of the file from NameNode.
		ArrayList<AllocatedBlock> blockToDataNodeMap = null;
		blockToDataNodeMap = this.nameSystemService.getFileBlocks(this.fileName);
		String res = "";

		// Get the content of each block sequentially.
		for (AllocatedBlock allocatedBlock : blockToDataNodeMap) {
			Block block = allocatedBlock.getBlock();

			Socket sock = null;
			ObjectInputStream ois = null;

			HashSet<DataNodeDescriptor> locations = allocatedBlock.getLocations();
			Iterator<DataNodeDescriptor> iterator = locations.iterator();
			DataNodeDescriptor dataNodeDescriptor = iterator.next();

			// Connect to the first DataNode.
			sock = new Socket(dataNodeDescriptor.getDataNodeIP(),
					dataNodeDescriptor.getDataNodePort());

			// Read a Block from DataNode
			ois = new ObjectInputStream(sock.getInputStream());
			DataNodePacket packet = (DataNodePacket) ois.readObject();
			ois.close();
			sock.close();

			ArrayList<String> lines = packet.getLines();

			for (String line : lines) {
				res += "\n" + line;
			}
		}

		return res.substring(1);
	}
}
