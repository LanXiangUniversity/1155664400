package lxu.lxdfs.namenode;

import lxu.lxdfs.BlocksLocation;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Wei on 11/4/14.
 */
public class ClientPacket implements Serializable {
	// Used for validate ack.
	private int packetID;
	// size of Block data(lines).
	private int len;
	// Data to be transferred.
	private List<String> lines;
	// Locations for each Block replica.
	private List<BlocksLocation> locations;
	// ID of this replica.
	private int replicaID;
	// Total replication Num.
	private int replicaNum;

	public int getPacketID() {
		return packetID;
	}

	public void setPacketID(int packetID) {
		this.packetID = packetID;
	}

	public int getReplicaNum() {
		return replicaNum;
	}

	public void setReplicaNum(int replicaNum) {
		this.replicaNum = replicaNum;
	}

	public int getLen() {
		return len;
	}

	public void setLen(int len) {
		this.len = len;
	}

	public List<String> getLines() {
		return lines;
	}

	public void setLines(List<String> lines) {
		this.lines = lines;
	}

	public List<BlocksLocation> getLocations() {
		return locations;
	}

	public void setLocations(List<BlocksLocation> locations) {
		this.locations = locations;
	}

	public int getReplicaID() {
		return replicaID;
	}

	public void setReplicaID(int replicaID) {
		this.replicaID = replicaID;
	}
}
