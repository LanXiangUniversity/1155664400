package lxu.lxdfs.metadata;

import java.io.Serializable;
import java.util.HashSet;

/**
 * Created by magl on 14/11/8.
 */
public class LocatedBlock implements Serializable {
	private Block block;
	private HashSet<DataNodeDescriptor> locations;

	public LocatedBlock(Block block, HashSet<DataNodeDescriptor> locations) {
		this.block = block;
		this.locations = locations;
	}

	public Block getBlock() {
		return block;
	}

	public void setBlock(Block block) {
		this.block = block;
	}

	public HashSet<DataNodeDescriptor> getLocations() {
		return locations;
	}

	public void setLocations(HashSet<DataNodeDescriptor> locations) {
		this.locations = locations;
	}
}
