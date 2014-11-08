package lxu.lxdfs.namenode;

import lxu.lxdfs.datanode.DataNodePacket;
import lxu.lxdfs.metadata.Block;
import lxu.lxdfs.metadata.DataNodeDescriptor;
import lxu.lxdfs.service.NameSystemService;

import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Wei on 11/2/14.
 */
public class NameNode {
	/**
	 * ********************** Data structures *********************
	 */
	private NameSystemService nameSystem;

	// List of Data Nodes available.
	private List<DataNodeDescriptor> dataNodes;
	// Map from file name to Block.
	private HashMap<String, Block> fileNameToBlockMap;
	// Map from Block to Data Nodes.
	private HashMap<Block, List<DataNodeDescriptor>> blockToLocationsMap;
	// Map from BlockID to Block
	private HashMap<Integer, Block> IDToBlockMap;

	/**
	 * ************************Services for client ************************
	 */

	public static void main(String[] args) {
		NameNode nameNode = new NameNode();

		// Regsiter and start RPC service.
		nameNode.registerService();


	}




	// Locating Block

	// File create

	// Block creation

	// Complete

	// Bad Blocks

	// Data node report

	// Blocks size

	// Safe mode

	// Name space

	// Meta data operations


	/**
	 * ************************Services for Data Node ************************
	 */
	// Registration

	// Block report

	// Block received

	// Bad Blocks

	// Generation time stamp

	// Version information

	// Block Synchronization update

	/**
	 *  Start up
	 */
	// 1. Loads default configurations and user modified configurations.

	// 2. Creates following servers to process user request.
	// a. RPC server
	// b. Http server to provide monitoring information
	// c. Trash emptier to manage user temporary files

	// 3. Initializes different metrics for monitoring

	// 4. Initializes Name System

	// 5. Threads for RPC server
	// a. Listener  -- listens for incoming request
	// b. Handler   -- handles user input and provides output Responder
	// c. Responder -- is responsible for respond to user request

	// 6. Thread for Name System (internal)
	// a. Heartbeat monitor     -- check whether data node is dead or running
	// b. Lease manager monitor -- check lease associated with client has expired or active
	// c. Replication monitor   -- monitors replication of blocks and replicate block as needed

	// 7. At startup Name Node is in Safe Mode (readonly mode for the HDFS).
	//      Block-Machine mapping is created each time Name Node starts and it is
	//      done by using data coming from different data nodes.
	//      It remains in Safe Mode until it has appropriate block information
	//      which satisfies the replication.

	// 8. Starts processing Client request after exiting Safe Mode.

	/**
	 * Return the locations<datanode, filename> of a Block
	 * @param blockID
	 * @return locations that store the Block
	 */
	public List<DataNodeDescriptor> getBlockLocations(int blockID) {
		List<DataNodeDescriptor> blockLocations = new ArrayList<DataNodeDescriptor>();

		// get Block by ID
		if (!IDToBlockMap.containsKey(blockID)) {
			return blockLocations;
		}
		Block block = IDToBlockMap.get(blockID);

		// get Data Node by Block
		if (!blockToLocationsMap.containsKey(block)) {
			return blockLocations;
		}

		List<DataNodeDescriptor> locations = blockToLocationsMap.get(block);

		return locations;
	}

	/**
	 * Storage
	 */
	// 1. File related information like name. replication factor etc.
	// 2. Block mapping for the file names.
	// 3. List of Data nodes available
	// 4. Blocks to data node mapping
	// 5. Network mtetrics
	// 6. Edit log
	public void registerService() {
		if (System.getSecurityManager() == null) {
			System.setSecurityManager(new RMISecurityManager());
		}
		try {
			NameSystemService nameSystem = new NameSystemService();

			Naming.rebind("rmi://localhost:56789/NameSystemService", nameSystem);
		} catch (Exception e) {
			System.out.println("Exception: " + e.getMessage());
			e.printStackTrace();
		}
	}
}
