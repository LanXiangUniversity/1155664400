package lxu.lxdfs.datanode;

import lxu.lxdfs.metadata.Block;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by magl on 14/11/8.
 */
public class DataNodePacket implements Serializable {
	private int ackPacketID;
	private Block block;
	private boolean operationState;
	private ArrayList<String> lines;

	public DataNodePacket(int ackPacketID,
	                      Block block,
	                      boolean operationState,
	                      ArrayList<String> lines) {
		this.ackPacketID = ackPacketID;
		this.block = block;
		this.operationState = operationState;
		this.lines = lines;
	}

	public int getAckPacketID() {
		return ackPacketID;
	}

	public void setAckPacketID(int ackPacketID) {
		this.ackPacketID = ackPacketID;
	}

	public Block getBlock() {
		return block;
	}

	public void setBlock(Block block) {
		this.block = block;
	}

	public boolean isOperationState() {
		return operationState;
	}

	public void setOperationState(boolean operationState) {
		this.operationState = operationState;
	}

	public ArrayList<String> getLines() {
		return lines;
	}

	public void setLines(ArrayList<String> lines) {
		this.lines = lines;
	}
}
