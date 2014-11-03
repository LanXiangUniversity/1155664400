package lxu.lxdfs;

/**
 * Created by Wei on 11/3/14.
 */
public class BlocksLocation {
	private DataNodeDescriptor dataNode;
	private String fileName;

	public DataNodeDescriptor getDataNode() {
		return dataNode;
	}

	public void setDataNode(DataNodeDescriptor dataNode) {
		this.dataNode = dataNode;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
}
