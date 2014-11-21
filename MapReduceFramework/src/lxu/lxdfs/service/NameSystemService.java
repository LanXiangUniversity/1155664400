package lxu.lxdfs.service;


import lxu.lxdfs.client.ClientOutputStream;
import lxu.lxdfs.metadata.*;
import lxu.lxdfs.namenode.NameNodeState;

import java.nio.file.Path;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.*;

/**
 * Remote object for Name System.
 * Created by Wei on 11/3/14.
 */
public class NameSystemService implements INameSystemService {
	// Index of DataNode to be allocated.
	private int dataAllocId = 0;
	private int nextDataNodeID = 0;
	// Path of root of the DFS
	private String rootPath;
	private int replicaNum = 1;
	private int blockID = 0;
	private Map<Integer, Long> lastResponseTime;
	// List of Data Nodes available.
	private Map<Integer, DataNodeDescriptor> dataNodes;
	// Map from file name to Block.
	private HashMap<String, List<Block>> fileNameToBlocksMap;
	// Map from Block to Data Nodes.
	private HashMap<Block, HashSet<DataNodeDescriptor>> blockToLocationsMap;
	// Map from BlockID to Block
	private HashMap<Integer, Block> IDToBlockMap;
	// List of file names.
	private Set<String> fileNames;
	private HashMap<String, List<Block>> deletedFiles;
	private NameNodeState nameNodeState = NameNodeState.STARTING;
	private static final int HEARTBEAT_TIMEOUT = 30 * 1000;

	public NameSystemService() {
		this.dataNodes = new HashMap<Integer, DataNodeDescriptor>();
		this.fileNameToBlocksMap = new HashMap<String, List<Block>>();
		this.blockToLocationsMap = new HashMap<Block, HashSet<DataNodeDescriptor>>();
		this.IDToBlockMap = new HashMap<Integer, Block>();
		this.fileNames = new HashSet<String>();
		this.lastResponseTime = new HashMap<>();

	}

	public boolean isSafeMode() {
		return this.nameNodeState != NameNodeState.OUT_OF_SAFE_MODE;
	}

	public void setSafeMode(boolean isSafeMode) {
		this.nameNodeState = NameNodeState.IN_SAFE_MODE;
	}

	/**
	 * Block write operations.
	 */
	public void enterSafeMode() {
		this.nameNodeState = NameNodeState.IN_SAFE_MODE;
	}

	public void exitSafeMode() {
		this.nameNodeState = NameNodeState.OUT_OF_SAFE_MODE;
	}

	// Remote services for Client.
	@Override
	public boolean mkdirs(Path path) throws RemoteException {
		return false;
	}

	/**
	 * Allocate two replicas for a Block with filename and Block offset.
	 *
	 * @param fileName
	 * @param offset   the blockId of in the file
	 * @return Locations for two blocks.
	 * @throws RemoteException
	 */
	@Override
	public LocatedBlock allocateBlock(String fileName, int offset) throws RemoteException {
		if (this.isSafeMode()) {
			return null;
		}
		System.out.println("allocate blocks for client filname: " + fileName);

		// Set unique global block ID.
		int blockId = this.blockID++;
		Block block = new Block(blockId, offset, 0L);
		HashSet<DataNodeDescriptor> locations = new HashSet<DataNodeDescriptor>();

		// TODO: Allocate DataNodes to store Block replicas.
        for (DataNodeDescriptor dataNodeDescriptor : dataNodes.values()) {
            locations.add(dataNodeDescriptor);
		//for (int i = 0; i < this.replicaNum; i++) {
			//locations.add(this.dataNodes.get(i));
		}

		// Register Blocks in NameNode.
		List<Block> fileBlocks = fileNameToBlocksMap.get(fileName);
		if (fileBlocks == null) {
			fileBlocks = new ArrayList<Block>();
			fileNameToBlocksMap.put(fileName, fileBlocks);
		}
		this.fileNameToBlocksMap.get(fileName).add(block);
		this.IDToBlockMap.put(blockId, block);

		// Register Blocks locations
		this.blockToLocationsMap.put(block, locations);

		return new LocatedBlock(block, locations);
	}

	/**
	 * Open a file and get the its outputStream.
	 *
	 * @param path
	 * @return
	 * @throws RemoteException
	 */
	@Override
	public ClientOutputStream open(Path path) throws RemoteException, NotBoundException {
		String fileName = path.toString();

		// File doesn't exist.
		if (this.fileNames.contains(fileName)) {
			return null;
		}

		ClientOutputStream cos = new ClientOutputStream();
		cos.setFileName(fileName);

		return cos;
	}


	/**
	 * @param path
	 * @return
	 * @throws RemoteException
	 */
	@Override
	public ClientOutputStream create(Path path) throws RemoteException, NotBoundException {
		String fileName = path.toString();

		if (this.fileNames.contains(path)) {
			System.out.println("Cannot create file, file exists");

			return null;
		}

		// Resgiter file.
		this.fileNames.add(fileName);
		this.fileNameToBlocksMap.put(fileName, new ArrayList<Block>());

		// Create ClientOutputStream
		ClientOutputStream cos = new ClientOutputStream();
		cos.setFileName(fileName);

		return cos;
	}

	@Override
	public boolean delete(Path path) throws RemoteException {
		if (this.isSafeMode()) {
			return false;
		}

		this.fileNames.remove(path);
		this.deletedFiles.put(path.toString(), this.fileNameToBlocksMap.get(path));
		this.fileNameToBlocksMap.remove(path);

		return true;
	}

	@Override
	public boolean exists(Path path) throws RemoteException {
		return this.fileNames.contains(path.toString());
	}

	@Override
	/**
	 * Return the replicas' locations of a Block
	 *
	 * @param blockID
	 * @return locations that store the Block
	 */
	public HashSet<DataNodeDescriptor> getBlockLocations(int blockID) throws RemoteException {
		HashSet<DataNodeDescriptor> blockLocations = new HashSet<DataNodeDescriptor>();

		// get Block by ID
		if (!IDToBlockMap.containsKey(blockID)) {
			return blockLocations;
		}
		Block block = IDToBlockMap.get(blockID);

		// get Data Node by Block
		if (!blockToLocationsMap.containsKey(block)) {
			return blockLocations;
		}
		HashSet<DataNodeDescriptor> locations = blockToLocationsMap.get(block);

		return locations;
	}

	/**
	 * Get the allocated Blocks of the file.
	 *
	 * @param fileName
	 * @return
	 * @throws RemoteException
	 */
	@Override
	public LocatedBlocks getFileBlocks(String fileName) throws RemoteException {
		// Get the Blocks of a file.
		List<Block> blocks = this.fileNameToBlocksMap.get(fileName);

		ArrayList<LocatedBlock> result = new ArrayList<LocatedBlock>();

		// Get the replicas' locations for each Block.
		for (Block block : blocks) {
			HashSet<DataNodeDescriptor> dataNodes = this.blockToLocationsMap.get(block);

			result.add(new LocatedBlock(block, dataNodes));
		}

		return new LocatedBlocks(result.toArray(new LocatedBlock[result.size()]));
	}

	/**
	 * Data Node register to the NameNode.
	 */
	@Override
	public int register(String dataNodeHostName, int port, ArrayList<Block> blocks) {
		if (this.nameNodeState == NameNodeState.OUT_OF_SAFE_MODE) {
			this.nameNodeState = NameNodeState.IN_SAFE_MODE;
		}

		this.nextDataNodeID++;
		System.out.println(dataNodeHostName + " registered");
		DataNodeDescriptor dataNode = new DataNodeDescriptor(nextDataNodeID,
				dataNodeHostName,
				port,
				blocks.size());
		// update dataNodes list
        this.dataNodes.put(nextDataNodeID, dataNode);
		//this.dataNodes.add(dataNode);

	    /* TODO return <**fileName**, blocksID, replicas> */

		// update blockToLocationsMap
		for (Block block : blocks) {
			HashSet<DataNodeDescriptor> dataNodeDescriptorSet = blockToLocationsMap.get(block);
			if (dataNodeDescriptorSet == null) {
				dataNodeDescriptorSet = new HashSet<DataNodeDescriptor>();
			}
			dataNodeDescriptorSet.add(dataNode);
			blockToLocationsMap.put(block, dataNodeDescriptorSet);
		}

		// Exit safe mode.
		this.nameNodeState = NameNodeState.OUT_OF_SAFE_MODE;

		return this.nextDataNodeID;
	}

    @Override
    public LinkedList<DataNodeCommand> heartbeat(int dataNodeID) throws RemoteException {
	    DataNodeDescriptor dataNode = new DataNodeDescriptor(dataNodeID);

	    LinkedList<DataNodeCommand> commands = new LinkedList<DataNodeCommand>();

	    long currentTime = System.currentTimeMillis();

	    if (!this.lastResponseTime.containsKey(dataNodeID)) {
		    this.lastResponseTime.put(dataNodeID, currentTime);
	    } else {
		    long lastResponseTime  = this.lastResponseTime.get(dataNodeID);

		    if ((currentTime - lastResponseTime) > this.HEARTBEAT_TIMEOUT) {
			    // Delete all the metadata about this dataNode.
				this.lastResponseTime.remove(dataNodeID);
			    this.dataNodes.remove(dataNodeID);

			    for (Block blk : this.blockToLocationsMap.keySet()) {
				    //List
				    //if (th)
			    }

			    // Create new replicas for the blocks on this node.


		    } else {
			    this.lastResponseTime.put(dataNodeID, currentTime);
				for(String fileName : this.deletedFiles.keySet()) {
					List<Block> blocks = this.deletedFiles.get(fileName);

					// Get each block of the deleted files.
					for (Block blk : blocks) {
						HashSet<DataNodeDescriptor> locations = this.blockToLocationsMap.get(blk);

						// Set delete command if dataNode has replica of this node.
						if (locations.contains(dataNode)) {
							DataNodeCommand command = new DataNodeDeleteCommand(blk);
							commands.add(command);
							this.blockToLocationsMap.get(blk).remove(dataNode);

							// Delete block if its location is empty.
							if (this.blockToLocationsMap.size() == 0) {
								this.blockToLocationsMap.remove(blk);
								this.deletedFiles.get(fileName).remove(blk);

								// Delete file if its blocks is empty.
								if (this.deletedFiles.get(fileName).size() == 0) {
									this.deletedFiles.remove(fileName);
								}
							}
						}
					}
				}
		    }
 	    }


        return commands;
    }
}
