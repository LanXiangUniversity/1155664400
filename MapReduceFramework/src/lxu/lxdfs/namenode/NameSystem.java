package lxu.lxdfs.namenode;

import lxu.lxdfs.Block;
import lxu.lxdfs.BlocksLocation;
import lxu.lxdfs.DataNodeDescriptor;

import java.nio.file.Path;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Remote object for Name System.
 * Created by Wei on 11/3/14.
 */
public class NameSystem implements INameSystem {
	private int dataAllocId = 0;
	// Path of root of the DFS
	private String rootPath;
	private int replicaNum = 2;
	private int blockID = 0;
	// List of Data Nodes available.
	private List<DataNodeDescriptor> dataNodes;
	// Map from file name to Block.
	private HashMap<String, List<Block>> fileNameToBlocksMap;
	// Map from Block to Data Nodes.
	private HashMap<Block, List<BlocksLocation>> blockToLocationsMap;
	// Map from BlockID to Block
	private HashMap<Integer, Block> IDToBlockMap;
	// List of file names.
	private Set<String> fileNames;

	public NameSystem() {
		DataNodeDescriptor dnd1 = new DataNodeDescriptor();
		dnd1.setDataNodeID(1);

		DataNodeDescriptor dnd2 = new DataNodeDescriptor();
		dnd2.setDataNodeID(2);

		this.dataNodes.add(dnd1);
		this.dataNodes.add(dnd2);
	}

	// Remote services for Client.
	@Override
	public boolean mkdirs(Path path) throws RemoteException {
		return false;
	}

	@Override
	public List<Block> allocateBlock(String fileName, int offset) throws RemoteException {
		Block block = new Block();
		int blockId = this.blockID++;
		List<BlocksLocation> locations = new ArrayList<BlocksLocation>();

		for (int i = 0; i < this.replicaNum; i++) {
			BlocksLocation location = new BlocksLocation();


			locations.add(location);
		}

		this.fileNameToBlocksMap.get(fileName).add(block);
		this.IDToBlockMap.put(blockId, block);
		this.blockToLocationsMap.put(block, locations);

		this.blockID++;
		return null;
	}

	@Override
	public boolean open(Path path) throws RemoteException {
		String fileName = this.rootPath + path.toString();

		if (this.fileNames.contains(fileName)) {
			return false;
		}

		return false;
	}


	@Override
	public DFSOutputStream create(Path path) throws RemoteException {
		if (this.fileNames.contains(path)) {
			System.out.println("Cannot create file, file exists");

			return null;
		}

		return new DFSOutputStream();
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
	public List<BlocksLocation> getBlockLocations(int blockID) throws RemoteException {
		List<BlocksLocation> blockLocations = new ArrayList<BlocksLocation>();

		// get Block by ID
		if (!IDToBlockMap.containsKey(blockID)) {
			return blockLocations;
		}
		Block block = IDToBlockMap.get(blockID);

		// get Data Node by Block
		if (!blockToLocationsMap.containsKey(block)) {
			return blockLocations;
		}

		List<BlocksLocation> locations = blockToLocationsMap.get(block);

		return locations;
	}
}
