package lxu.lxdfs;

import java.util.List;

/**
 * Created by Wei on 11/3/14.
 */
public class Block {
	private Long blockID;              // global Block ID
	private long len;                  // size of bytes

    public long getLen() {
        return len;
    }

    public void setLen(long len) {
        this.len = len;
    }

    public Long getBlockID() {
        return blockID;
    }

    public void setBlockID(Long blockID) {
        this.blockID = blockID;
    }
}
