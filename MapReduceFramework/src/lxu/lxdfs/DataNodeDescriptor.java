package lxu.lxdfs;

/**
 * This class stores the meta-data of a Data Node.
 */
public class DataNodeDescriptor {
	private int dataNodeID;
	private String dataNodeIP;
	private int dataNodePort;

	public int getBlockNum() {
		return blockNum;
	}

	public void setBlockNum(int blockNum) {
		this.blockNum = blockNum;
	}

	public int getDataNodeID() {
		return dataNodeID;
	}

	public void setDataNodeID(int dataNodeID) {
		this.dataNodeID = dataNodeID;
	}

	public String getDataNodeIP() {
		return dataNodeIP;
	}

	public void setDataNodeIP(String dataNodeIP) {
		this.dataNodeIP = dataNodeIP;
	}

	public int getDataNodePort() {
		return dataNodePort;
	}

	public void setDataNodePort(int dataNodePort) {
		this.dataNodePort = dataNodePort;
	}

	// # Blocks in the Data Node
	private int blockNum;

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		DataNodeDescriptor that = (DataNodeDescriptor) o;

		if (dataNodeID != that.dataNodeID) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return dataNodeID;
	}
}