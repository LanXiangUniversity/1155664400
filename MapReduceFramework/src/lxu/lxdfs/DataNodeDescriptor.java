package lxu.lxdfs;

/**
 * This class stores the meta-data of a Data Node.
 */
public class DataNodeDescriptor {
	private int dataNodeID;
	private String dataNodeIP;
	private int dataNodePort;

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
