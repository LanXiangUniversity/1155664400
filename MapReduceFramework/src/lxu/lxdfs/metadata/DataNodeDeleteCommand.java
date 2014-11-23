package lxu.lxdfs.metadata;

/**
 * DataNodeDeleteCommand.java
 * Created by Wei on 11/21/14.
 *
 * Delete a block.
 */
public class DataNodeDeleteCommand extends DataNodeCommand {
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
