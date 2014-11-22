package lxu.lxmapreduce.io;

import java.io.*;
import java.net.Socket;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import lxu.lxdfs.client.ClientPacket;
import lxu.lxdfs.datanode.DataNode;
import lxu.lxdfs.datanode.DataNodePacket;
import lxu.lxdfs.metadata.Block;
import lxu.lxdfs.metadata.DataNodeDescriptor;
import lxu.lxdfs.metadata.LocatedBlock;
import lxu.lxdfs.metadata.LocatedBlocks;
import lxu.lxmapreduce.io.format.LongWritable;
import lxu.lxmapreduce.io.format.Text;
import lxu.lxmapreduce.tmp.TaskAttemptContext;

/**
 * Treats keys as offset in file and value as line.
 * Created by Wei on 11/11/14.
 */
public class LineRecordReader extends RecordReader<LongWritable, Text> {

	private int lineNum = 0;
	private LineReader in;
	private LongWritable key = null;
	private int currentSplit;
	private int maxSplit;
	private Text value = null;
	private List<String> inputFiles;
	private List<LocatedBlock> locatedBlockses;

	public LineRecordReader() {
		/* TODO Init LineReader */
		if (this.key == null) {
			this.key = new LongWritable();
		}

		if (this.value == null) {
			this.value = new Text();
		}
	}

	@Override
	public void initialize(List<String> inputFiles, List<LocatedBlock> locatedBlockses) throws IOException {
		this.currentSplit = 0;
		this.maxSplit = inputFiles.size() - 1;
		this.inputFiles = inputFiles;
		this.locatedBlockses = locatedBlockses;

		getInputSplit();
	}

	@Override
	public Text getCurrentValue() {
		return this.value;
	}

	@Override
	public LongWritable getCurrentKey() {
		return this.key;
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		int res = 0;
		boolean res1 = true;

		this.key.set(this.lineNum++);
		res = in.readLine(this.value);
		if (res == 0) {
			res1 = getInputSplit();
		}

		return (res != 0) && res1;
	}

	private boolean getInputSplit() throws IOException {
		if (this.currentSplit <= this.maxSplit) {

			// Local replcia ?
			File file  = new File(inputFiles.get(this.currentSplit));
			// If there is no files in the localhost then get from remote data node.
			if (!file.exists()) {
				// TODO: read from remote.
				for (DataNodeDescriptor dataNode : this.locatedBlockses.get(this.currentSplit).getLocations()) {
					boolean isFailed = false;
					Socket sock = null;
					ObjectOutputStream oos = null;
					ObjectInputStream ois = null;

					try {
						// Connect to the first DataNode.
						sock = new Socket(dataNode.getDataNodeIP(), dataNode.getDataNodePort());
						// Read a Block from DataNode
						oos = new ObjectOutputStream(sock.getOutputStream());
						oos.writeObject(generateReadPacket(this.locatedBlockses.get(this.currentSplit).getBlock()));
						System.out.println("read block from remote");
						ois = new ObjectInputStream(sock.getInputStream());
						DataNodePacket packet = (DataNodePacket) ois.readObject();
						ois.close();
						sock.close();

						List<String> lines = packet.getLines();
						PrintWriter pw = new PrintWriter(new FileWriter(this.inputFiles.get(this.currentSplit)));
						for (String line : lines) {
							pw.println(line);
						}
						pw.close();
						isFailed = false;
					} catch(Exception e) {
						isFailed = true;
						e.printStackTrace();
					}

					if (!isFailed) {
						if (this.currentSplit > 0) this.in.close();
						this.in = new LineReader(inputFiles.get(this.currentSplit));
						this.currentSplit++;
						return true;
					}
				}
			} else {
				if (this.currentSplit > 0) this.in.close();
				this.in = new LineReader(inputFiles.get(this.currentSplit));
				this.currentSplit++;
				return true;
			}

		}

		return false;
	}

	public ClientPacket generateReadPacket(Block block) {
		ClientPacket packet = new ClientPacket();
		packet.setOperation(ClientPacket.BLOCK_READ);
		packet.setBlock(block);
		return packet;
	}

	@Override
	public void close() throws IOException {
		if (this.in != null) {
			in.close();
		}
	}
}
