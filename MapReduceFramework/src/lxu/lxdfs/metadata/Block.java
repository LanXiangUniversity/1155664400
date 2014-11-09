package lxu.lxdfs.metadata;

import java.util.List;

/**
 * Created by Wei on 11/3/14.
 */
public class Block {
	private Long blockID;              // global Block ID
    private long offset;
	private long len;                  // size of bytes

    public long getLen() {
        return len;
    }

    public void setLen(long len) {
        this.len = len;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
    public Long getBlockID() {
        return blockID;
    }

    public void setBlockID(Long blockID) {
        this.blockID = blockID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Block block = (Block) o;

        return blockID.equals(block.blockID);

    }

    @Override
    public int hashCode() {
        return blockID.hashCode();
    }
}
