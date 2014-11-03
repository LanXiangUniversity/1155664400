package lxu.communication;

/**
 * Created by Wei on 11/1/14.
 */

/**
 * This is used transfer data between data nodes and from DFS client
 * to data node. It is by default 64K of size. Following are the main
 * things a packet contains.
 * a) Packet length
 * b) Packet sequence number
 * c) Offset in block
 * e) Whether this is last packet of the block?
 * (writes 0 to data node) o Data Length (excluding checksum)
 */

public class Packet {
}
