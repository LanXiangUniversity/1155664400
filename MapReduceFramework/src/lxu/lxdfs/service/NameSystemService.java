package lxu.lxdfs.service;


import lxu.lxdfs.client.ClientOutputStream;
import lxu.lxdfs.metadata.*;
import lxu.lxdfs.namenode.NameNodeState;

import java.nio.file.Path;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Remote object for Name System.
 * Created by Wei on 11/3/14.
 */
public class NameSystemService implements INameSystemService {
	private static final int HEARTBEAT_TIMEOUT = 30 * 1000;
	private static final int REPLICA_NUM = 2;
	// Index of DataNode to be allocated.
	private int nextDataNodeID = 0;
	// Path of root of the DFS
	private String rootPath;
	private int blockID = 0;
	private ConcurrentHashMap<DataNodeDescriptor, Long> lastResponseTime;
	// List of Data Nodes available.
	private ConcurrentHashMap<Integer, DataNodeDescriptor> dataNodes;
	// Map from file name to Block.
	private ConcurrentHashMap<String, List<Block>> fileNameToBlocksMap;
	// Map from Block to Data Nodes.
	private ConcurrentHashMap<Block, HashSet<DataNodeDescriptor>> blockToLocationsMap;
	// Map from BlockID to Block.
	private ConcurrentHashMap<Integer, Block> IDToBlockMap;
	// List of file names.
	private HashSet<String> fileNames;
	private ConcurrentHashMap<String, List<Block>> deletedFiles;
	private ConcurrentHashMap<DataNodeDescriptor, ArrayList<Block>> restoreBlocksQueue;
	private NameNodeState nameNodeState = NameNodeState.STARTING;
	private DataNodeTimeoutListener dataNodeTimeoutListener;

	public NameSystemService() {
		this.dataNodes = new ConcurrentHashMap<>();
		this.fileNameToBlocksMap = new ConcurrentHashMap<String, List<Block>>();
		this.blockToLocationsMap = new ConcurrentHashMap<Block, HashSet<DataNodeDescriptor>>();
		this.IDToBlockMap = new ConcurrentHashMap<Integer, Block>();
		this.fileNames = new HashSet<>();
		this.lastResponseTime = new ConcurrentHashMap<>();
		this.deletedFiles = new ConcurrentHashMap<>();
		new Thread(this.dataNodeTimeoutListener).start();

	}

	public synchronized boolean isSafeMode() {
		return this.nameNodeState != NameNodeState.OUT_OF_SAFE_MODE;
	}

	public synchronized void setSafeMode(boolean isSafeMode) {
		this.nameNodeState = NameNodeState.IN_SAFE_MODE;
	}

	/**
	 * Block write operations.
	 */
	public synchronized void enterSafeMode() {
		this.nameNodeState = NameNodeState.IN_SAFE_MODE;
	}

	public synchronized void exitSafeMode() {
		this.nameNodeState = NameNodeState.OUT_OF_SAFE_MODE;
	}

	// Remote services for Client.
	@Override
	public synchronized boolean mkdirs(Path path) throws RemoteException {
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
	public synchronized LocatedBlock allocateBlock(String fileName, int offset) throws RemoteException {
		if (this.isSafeMode()) {
			return null;
		}
		System.out.println("allocate blocks for client filname: " + fileName);

		// Set unique global block ID.
		int blockId = this.blockID++;
		Block block = new Block(blockId, offset, 0L);
		HashSet<DataNodeDescriptor> locations = new HashSet<DataNodeDescriptor>();

		// TODO: Allocate DataNodes to store Block replicas.
		for (int i = 0; i < this.REPLICA_NUM; i++) {
			int min = Integer.MAX_VALUE;
			DataNodeDescriptor dn = this.dataNodes.entrySet().iterator().next().getValue();
			for (int dnId : this.dataNodes.keySet()) {
				DataNodeDescriptor tmpDataNode = this.dataNodes.get(dnId);

				if (tmpDataNode.getBlockNum() < min &&
						(!locations.contains(tmpDataNode))) {
					min = tmpDataNode.getBlockNum();
					dn = tmpDataNode;
					break;
				}
			}

			// Update status of dataNode.
			locations.add(dn);
			// Increase data number by one.
			this.dataNodes.get(dn.getDataNodeID()).setBlockNum(this.dataNodes.get(dn.getDataNodeID())
					.getBlockNum()+1);
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
	public synchronized ClientOutputStream open(Path path) throws RemoteException, NotBoundException {
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
	 * @param fileName
	 * @return
	 * @throws RemoteException
	 */
	@Override
	public synchronized void create(String fileName) throws RemoteException, NotBoundException {

		if (this.fileNames.contains(fileName)) {
			System.out.println("Cannot create file, file exists");

			return;
		}

		// Resgiter file.
		System.out.println("Create file, filename " + fileName);
		this.fileNames.add(fileName);
		this.fileNameToBlocksMap.put(fileName, new ArrayList<Block>());


		return;
	}

	@Override
	public synchronized boolean delete(String fileName) throws RemoteException {
		if (this.isSafeMode()) {
			return false;
		}

		this.fileNames.remove(fileName);
		this.deletedFiles.put(fileName, this.fileNameToBlocksMap.get(fileName));
		this.fileNameToBlocksMap.remove(fileName);

		return true;
	}

	@Override
	public synchronized boolean exists(String filename) throws RemoteException {
		return this.fileNames.contains(filename);
	}

	@Override
	/**
	 * Return the replicas' locations of a Block
	 *
	 * @param blockID
	 * @return locations that store the Block
	 */
	public synchronized HashSet<DataNodeDescriptor> getBlockLocations(int blockID) throws RemoteException {
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
	public synchronized LocatedBlocks getFileBlocks(String fileName) throws RemoteException {
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
	public synchronized int register(String dataNodeHostName, int port, ArrayList<Block> blocks) {
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
    public synchronized LinkedList<DataNodeCommand> heartbeat(DataNodeDescriptor dataNode) throws RemoteException {

	    LinkedList<DataNodeCommand> commands = new LinkedList<DataNodeCommand>();

	    long currentTime = System.currentTimeMillis();
	    this.lastResponseTime.put(dataNode, currentTime);


	    if (this.restoreBlocksQueue.keySet().contains(dataNode)) {
		    for (Block blk : this.restoreBlocksQueue.get(dataNode)) {
			    HashSet<DataNodeDescriptor> srcDataNode =
					                        this.blockToLocationsMap.get(blk);
			    Iterator<DataNodeDescriptor> iter = srcDataNode.iterator();

			    DataNodeDescriptor dn1 = iter.next();

			    DataNodeRestoreCommand command = new DataNodeRestoreCommand(blk, dn1);

			    commands.add(command);
		    }
	    } else {
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

							this.dataNodes.get(dataNode.getDataNodeID()).setBlockNum(this.dataNodes.get(dataNode.getDataNodeID())
									.getBlockNum() - 1);
						}
					}
				}
 	    }

        return commands;
    }

	@Override
	public synchronized HashSet<String> ls() throws RemoteException {
		return this.fileNames;
	}

	class DataNodeTimeoutListener implements Runnable {
		@Override
		public void run() {
			while (nameNodeState != NameNodeState.IN_SAFE_MODE) {
				try {
					long currentTime = System.currentTimeMillis();

					// Check every dataNode.
					for (DataNodeDescriptor dataNode : lastResponseTime.keySet()) {
						long lastTime = lastResponseTime.get(dataNode);

						if ((currentTime - lastTime) > HEARTBEAT_TIMEOUT) {
							// Delete all the metadata about this dataNode.
							lastResponseTime.remove(dataNode);
							dataNodes.remove(dataNode.getDataNodeID());

							for (Block blk : blockToLocationsMap.keySet()) {
								Set<DataNodeDescriptor> locations = blockToLocationsMap.get(blk);

								// Keep all blocks of this data node into a queue.
								if (locations.contains(dataNode)) {
									locations.remove(dataNode);

									// Get data node which has minimum blocks number.
									// And has no replica of then block.
									int min = Integer.MAX_VALUE;
									DataNodeDescriptor dn = dataNodes.entrySet().iterator().next().getValue();
									for (int dnId : dataNodes.keySet()) {
										DataNodeDescriptor tmpDataNode = dataNodes.get(dnId);

										if (tmpDataNode.getBlockNum() < min &&
												(!blockToLocationsMap.get(blk).contains(tmpDataNode)
												|| blockToLocationsMap.get(blk) == null
												|| blockToLocationsMap.get(blk).size() == 0)) {
											min = tmpDataNode.getBlockNum();
											dn = tmpDataNode;
											break;
										}
									}

									// Update status of dataNode.
									locations.add(dn);
									// Increase data number by one.
									dataNodes.get(dn.getDataNodeID()).setBlockNum(dataNodes.get(dn.getDataNodeID())
											.getBlockNum() + 1);

									if (restoreBlocksQueue.get(dn) == null) {
										restoreBlocksQueue.put(dn, new ArrayList<Block>());
									}

									ArrayList<Block> blocks = restoreBlocksQueue.get(dn);
									blocks.add(blk);
									restoreBlocksQueue.put(dn, blocks);
								}
							}
						}
					}

					Thread.sleep(HEARTBEAT_TIMEOUT);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
