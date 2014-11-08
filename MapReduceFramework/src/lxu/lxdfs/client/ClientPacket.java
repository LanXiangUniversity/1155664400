package lxu.lxdfs.client;

import lxu.lxdfs.metadata.BlocksLocation;
import lxu.lxdfs.metadata.DataNodeDescriptor;

import java.io.Serializable;
import java.util.ArrayList;
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
	private ArrayList<String> lines;
	// Locations for each Block replica.
	private ArrayList<DataNodeDescriptor> locations;
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

	public ArrayList<String> getLines() {
		return lines;
	}

	public void setLines(ArrayList<String> lines) {
		this.lines = lines;
	}

	public ArrayList<DataNodeDescriptor> getLocations() {
		return locations;
	}

	public void setLocations(ArrayList<DataNodeDescriptor> locations) {
		this.locations = locations;
	}

	public int getReplicaID() {
		return replicaID;
	}

	public void setReplicaID(int replicaID) {
		this.replicaID = replicaID;
	}
}
