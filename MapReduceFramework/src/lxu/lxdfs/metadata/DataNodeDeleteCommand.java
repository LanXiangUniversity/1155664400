package lxu.lxdfs.metadata;

/**
 * Created by Wei on 11/21/14.
 */
public class DataNodeDeleteCommand extends DataNodeCommand{
	private Block block;
	public DataNodeDeleteCommand(Block block) {
		this.type = this.DELETE_BLOCK;
		this.block = block;
	}

    public Block getBlock() {
        return block;
    }

    public void setBlock(Block block) {
        this.block = block;
    }
}
