package lxu.lxdfs.metadata;

import java.io.Serializable;

/**
 * Created by magl on 14/11/19.
 */
public class DataNodeCommand implements Serializable {
	protected int type;
	protected static final int DELETE_BLOCK = 0;
	protected static final int RESTORE_BLOCK = 1;

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }
}
