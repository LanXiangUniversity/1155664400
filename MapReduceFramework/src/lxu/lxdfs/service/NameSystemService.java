package lxu.lxdfs.service;


import lxu.lxdfs.client.ClientOutputStream;
import lxu.lxdfs.metadata.AllocatedBlock;
import lxu.lxdfs.metadata.Block;
import lxu.lxdfs.metadata.DataNodeDescriptor;

import java.nio.file.Path;
import java.rmi.RemoteException;
import java.util.*;

/**
 * Remote object for Name System.
 * Created by Wei on 11/3/14.
 */
public class NameSystemService implements INameSystemService {
	// Index of DataNode to be allocated.
	private int dataAllocId = 0;
    private int nextDataNodeID = 1;
	// Path of root of the DFS
	private String rootPath;
	private int replicaNum = 2;
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


	public NameSystemService() {
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
	public AllocatedBlock allocateBlock(String fileName, int offset) throws RemoteException {
		// Set unique global block ID.
		int blockId = this.blockID++;
        Block block = new Block(blockId, offset, 0L);
		HashSet<DataNodeDescriptor> locations = new HashSet<DataNodeDescriptor>();

		// Allocate DataNodes to store Block replicas.
		for (int i = 0; i < this.replicaNum; i++) {
			locations.add(this.dataNodes.get(i));
		}

		// Register Blocks in NameNode.
		this.fileNameToBlocksMap.get(fileName).add(block);
		this.IDToBlockMap.put(blockId, block);

		// Register Blocks locations
		this.blockToLocationsMap.put(block, locations);

		return new AllocatedBlock(block, locations);
	}

	/**
	 * Open a file and get the its outputStream.
	 * @param path
	 * @return
	 * @throws RemoteException
	 */
	@Override
	public ClientOutputStream open(Path path) throws RemoteException {
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
	public ClientOutputStream create(Path path) throws RemoteException {
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
	public ArrayList<AllocatedBlock> getFileBlocks(String fileName) throws RemoteException {
		// Get the Blocks of a file.
		List<Block> blocks = this.fileNameToBlocksMap.get(fileName);
		ArrayList<AllocatedBlock> result = new ArrayList<AllocatedBlock>();

		// Get the replicas' locations for each Block.
		for (Block block : blocks) {
			HashSet <DataNodeDescriptor> dataNodes = this.blockToLocationsMap.get(block);

			result.add(new AllocatedBlock(block, dataNodes));
		}

		return result;
	}

	/**
     * Data Node register to the NameNode.
     */
    @Override
    public boolean register(String dataNodeHostName, int port, ArrayList<Block> blocks) {
        DataNodeDescriptor dataNode = new DataNodeDescriptor(nextDataNodeID,
                                                             dataNodeHostName,
                                                             port,
                                                             blocks.size());
        // update dataNodes list
        this.dataNodes.add(dataNode);

        // update blockToLocationsMap
        for (Block block : blocks) {
            HashSet<DataNodeDescriptor> dataNodeDescriptorSet = blockToLocationsMap.get(block);
            dataNodeDescriptorSet.add(dataNode);
            blockToLocationsMap.put(block, dataNodeDescriptorSet);
        }
        this.nextDataNodeID++;

	    return true;
    }
}
