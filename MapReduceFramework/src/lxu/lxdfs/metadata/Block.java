package lxu.lxdfs.metadata;

import java.io.Serializable;

/**
 * Block.java
 * Created by Wei on 11/3/14.
 *
 * A abstraction to a file block.
 */
public class Block implements Serializable {
    private Long blockID;              // global Block ID
    private long offset;
    private long len;                  // size of bytes

    public Block() {
        blockID = 0L;
        offset = 0L;
        len = 0L;
    }

    public Block(long blockID, long offset, long len) {
        this.blockID = blockID;
        this.offset = offset;
        this.len = len;
    }

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
