package lxu.lxdfs;

import java.util.List;

/**
 * Created by Wei on 11/3/14.
 */
public class Block {
	private long blockID;              // global Block ID
	private long offset;               // the offset in the file
	private long len;                  // size of bytes

	private String fileName;
	private List<DataNodeDescriptor> dataNodes;          // dataNodeID where the Block is stored
	private List<String> dataNodeFileNames;               // Filename of the Block
}
