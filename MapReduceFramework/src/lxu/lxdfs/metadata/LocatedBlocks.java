package lxu.lxdfs.metadata;

import java.io.Serializable;

/**
 * LocatedBlocks.java
 * Created by magl on 14/11/11.
 *
 * This class is useful for client to locate all blocks of a file.
 */
public class LocatedBlocks implements Serializable {
    private LocatedBlock[] blocks;

    public LocatedBlocks(LocatedBlock[] blocks) {
        this.blocks = blocks;
    }

    public LocatedBlock[] getBlocks() {
        return blocks;
    }

    public void setBlocks(LocatedBlock[] blocks) {
        this.blocks = blocks;
    }
}
