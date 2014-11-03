package lxu.lxdfs;

import java.util.List;

/**
 * Created by Wei on 11/3/14.
 */
public class Block {
	private int blockID;                // global Block ID
	private int offset;                 // the offset in the file

	private String fileName;
	private List<DataNodeInfo> dataNodes;          // dataNodeID where the Block is stored
	private List<String> dataNodeFileNames;               // Filename of the Block
}
