package lxu.lxdfs.client;

import lxu.lxdfs.datanode.DataNodePacket;
import lxu.lxdfs.metadata.Block;
import lxu.lxdfs.metadata.DataNodeDescriptor;
import lxu.lxdfs.service.NameSystemService;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

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
		// Get  Blocks metadata of the file from NameNode.
		HashMap<Block, ArrayList<DataNodeDescriptor>> blockToDataNodeMap = this.nameSystemService.getFileBlocks();
		String res = "";

		// Get the content of each block sequentially.
		for (Block block : blockToDataNodeMap.keySet()) {
			Socket sock = null;
			ObjectInputStream ois = null;

			ArrayList<DataNodeDescriptor> locations = blockToDataNodeMap.get(block);

			sock = new Socket(locations.get(0).getDataNodeIP(),
					locations.get(0).getDataNodePort());

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
