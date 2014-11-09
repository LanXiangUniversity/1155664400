package lxu.lxdfs.client;

import lxu.lxdfs.datanode.DataNode;
import lxu.lxdfs.metadata.Block;
import lxu.lxdfs.metadata.DataNodeDescriptor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Set;

/**
 * Created by Wei on 11/4/14.
 */
public class ClientPacket implements Serializable {
	public static final int BLOCK_READ = 0;
	public static final int BLOCK_WRITE = 1;
	// Used for validate ack.
	private int packetID;
	// Target block
	private Block block;
	// Data to be transferred.
	private ArrayList<String> lines;
	// Locations for each Block replica.
	//private Set<DataNodeDescriptor> locations;
    private DataNodeDescriptor location;
	// ID of this replica.
	private int replicaID;
	// Total replication Num.
	private int replicaNum;
	// Client operation.
	// Possible operation: read (1), write (2)
	private int operation;

	public int getOperation() {
		return operation;
	}

	public void setOperation(int operation) {
		this.operation = operation;
	}

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

	public Block getBlock() {
		return this.block;
	}

	public void setBlock(Block block) {
		this.block = block;
	}

	public ArrayList<String> getLines() {
		return lines;
	}

	public void setLines(ArrayList<String> lines) {
		this.lines = lines;
	}

	/*public Set<DataNodeDescriptor> getLocations() {
		return locations;
	}*/

    public DataNodeDescriptor getLocation() {
        return this.location;
    }

    public void setLocation(DataNodeDescriptor location) {
        this.location = location;
    }

	/*public void setLocations(Set<DataNodeDescriptor> locations) {
		this.locations = locations;
	}*/

	public int getReplicaID() {
		return replicaID;
	}

	public void setReplicaID(int replicaID) {
		this.replicaID = replicaID;
	}
}
