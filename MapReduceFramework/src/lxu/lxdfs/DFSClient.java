package lxu.lxdfs;

/**
 * Created by Wei on 11/1/14.
 */


import com.lxdfs.conf.Configuration;

import java.net.InetSocketAddress;
import java.nio.file.Path;

/**
 * Data nodes information
 * Packet creation
 * Packet communication
 *      Packet insertion to Data Queue
 *      Packet transfer from Data Queue to target data node
 *      Ack from packages
 *      Update ack queue in case of receive ack
 *
 */
public class DFSClient {
	/**
	 * Create a new DFSClient connected to the given nameNodeAddr or rpcNamenod
	 * @param nameNodeAddr
	 * @param conf
	 */
	public void DFSClient(InetSocketAddress nameNodeAddr) {

	}

	/*
		File operations.
	 */
	public FSDataInputStream open(Path f) {
		/**
		 *
		 */
		return new FSDataInputStream();
	}

	public FSDataOutputStream create(Path f) {
		/**
		 *
		 */
		return new FSDataOutputStream();
	}

	public boolean delete(Path f) {
		/**
		 *
		 */
		return false;
	}

	public boolean close(Path f) {
		/**
		 *
		 */
		return false;
	}

	public boolean exists(Path f) {
		/**
		 *
		 */
		return false;
	}

	public boolean mkdirs(Path f) {
		/**
		 *
		 */
		return false;
	}

	public boolean close() {
		/**
		 *
		 */
		return false;
	}
}
