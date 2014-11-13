package lxu.lxdfs.service;


import lxu.lxdfs.client.ClientOutputStream;
import lxu.lxdfs.metadata.LocatedBlock;
import lxu.lxdfs.metadata.Block;
import lxu.lxdfs.metadata.DataNodeDescriptor;
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
	// List of Data Nodes available.
	private List<DataNodeDescriptor> dataNodes;
	// Map from file name to Block.
	private HashMap<String, List<Block>> fileNameToBlocksMap;
	// Map from Block to Data Nodes.
	private HashMap<Block, HashSet<DataNodeDescriptor>> blockToLocationsMap;
	// Map from BlockID to Block
	private HashMap<Integer, Block> IDToBlockMap;
	// List of file names.
	private Set<String> fileNames;
	private NameNodeState nameNodeState = NameNodeState.STARTING;

	public NameSystemService() {
        dataNodes = new LinkedList<DataNodeDescriptor>();
        fileNameToBlocksMap = new HashMap<String, List<Block>>();
        blockToLocationsMap = new HashMap<Block, HashSet<DataNodeDescriptor>>();
        IDToBlockMap = new HashMap<Integer, Block>();
        fileNames = new HashSet<String>();
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
	public void enterSafeMode () {
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
		if (this.isSafeMode()) {return null;}
		System.out.println("allocate blocks for client filname: " + fileName );

		// Set unique global block ID.
		int blockId = this.blockID++;
        Block block = new Block(blockId, offset, 0L);
		HashSet<DataNodeDescriptor> locations = new HashSet<DataNodeDescriptor>();

		// Allocate DataNodes to store Block replicas.
		for (int i = 0; i < this.replicaNum; i++) {
			locations.add(this.dataNodes.get(i));
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
	 *
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
		if (this.isSafeMode()) {return false;}

		return false;
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
	 * @param fileName
	 * @return
	 * @throws RemoteException
	 */
	@Override
	public ArrayList<LocatedBlock> getFileBlocks(String fileName) throws RemoteException {
		// Get the Blocks of a file.
		List<Block> blocks = this.fileNameToBlocksMap.get(fileName);
		ArrayList<LocatedBlock> result = new ArrayList<LocatedBlock>();

		// Get the replicas' locations for each Block.
		for (Block block : blocks) {
			HashSet <DataNodeDescriptor> dataNodes = this.blockToLocationsMap.get(block);

			result.add(new LocatedBlock(block, dataNodes));
		}

		return result;
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
        this.dataNodes.add(dataNode);

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
}
