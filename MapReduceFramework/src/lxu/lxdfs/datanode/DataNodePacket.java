package lxu.lxdfs.datanode;

import lxu.lxdfs.Block;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by magl on 14/11/8.
 */
public class DataNodePacket implements Serializable {
    private int ackPacketID;
    private Block block;
    private boolean operationState;
    private ArrayList<String> lines;

    public DataNodePacket (int ackPacketID,
                           Block block,
                           boolean operationState,
                           ArrayList<String> lines) {
        this.ackPacketID = ackPacketID;
        this.block = block;
        this.operationState = operationState;
        this.lines = lines;
    }
}
