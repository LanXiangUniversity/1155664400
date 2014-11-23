package lxu.lxdfs.metadata;

import java.io.Serializable;

/**
 * Created by magl on 14/11/11.
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
