package lxu.lxdfs.service;


import lxu.lxdfs.client.ClientOutputStream;
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
	private HashMap<Block, Set<DataNodeDescriptor>> blockToLocationsMap;
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
	public Set<DataNodeDescriptor> allocateBlock(String fileName, int offset) throws RemoteException {
		Block block = new Block();
		int blockId = this.blockID++;
		Set<DataNodeDescriptor> locations = new HashSet<DataNodeDescriptor>();

		for (int i = 0; i < this.replicaNum; i++) {
			locations.add(this.dataNodes.get(i));
		}

		// Register blocks
		this.fileNameToBlocksMap.get(fileName).add(block);
		this.IDToBlockMap.put(blockId, block);
		// Store Blocks locations
		this.blockToLocationsMap.put(block, locations);

		this.blockID++;

		return locations;
	}

	@Override
	public ClientOutputStream open(Path path) throws RemoteException {
		/*
			To be implemented
		 */

		return null;
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

		return new ClientOutputStream();
	}

	@Override
	public boolean delete(Path path) throws RemoteException {
		return false;
	}

	@Override
	public boolean exists(Path path) throws RemoteException {
		return false;
	}

	@Override
    /**
     * Return the locations<datanode, filename> of a Block
     *
     * @param blockID
     * @return locations that store the Block
     */
	public Set<DataNodeDescriptor> getBlockLocations(int blockID) throws RemoteException {
		Set<DataNodeDescriptor> blockLocations = new HashSet<DataNodeDescriptor>();

		// get Block by ID
		if (!IDToBlockMap.containsKey(blockID)) {
			return blockLocations;
		}
		Block block = IDToBlockMap.get(blockID);

		// get Data Node by Block
		if (!blockToLocationsMap.containsKey(block)) {
			return blockLocations;
		}

		Set<DataNodeDescriptor> locations = blockToLocationsMap.get(block);

		return locations;
	}

    /**
     * Data Node
     */
    @Override
    public boolean register(String dataNodeHostName, int port, ArrayList<Block> blocks) {
        DataNodeDescriptor dataNode = new DataNodeDescriptor(nextDataNodeID,
                                                             dataNodeHostName,
                                                             port,
                                                             blocks.size());
        // update dataNodes list
        dataNodes.add(dataNode);
        // update blockToLocationsMap
        for (Block block : blocks) {
            Set<DataNodeDescriptor> dataNodeDescriptorSet = blockToLocationsMap.get(block);
            dataNodeDescriptorSet.add(dataNode);
            blockToLocationsMap.put(block, dataNodeDescriptorSet);
        }
        nextDataNodeID++;
        return true;
    }
}
