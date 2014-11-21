package lxu.lxdfs.datanode;

import lxu.lxdfs.metadata.*;
import lxu.lxdfs.service.INameSystemService;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.LinkedList;
import java.util.List;

/**
 * DataNode.java
 *
 * DataNode is a class that stores a set of blocks for DFS.
 * Each DataNode communicates regularly with a single NameNode.
 * It also communicates with client code and other DataNodes
 * from time to time. DataNodes store a series of named blocks.
 * The DataNode allows client code to read these blocks, or to
 * write new block data. The DataNode may also, in response to
 * instructions from its NameNode, delete blocks or copy blocks
 * to/from other DataNodes. The DataNode uses {@link BlockService}
 * to maintain blocks. DataNodes spend their lives in an endless
 * loop of asking the NameNode for something to do. A NameNode
 * cannot connect to a DataNode directly; a NameNode simply returns
 * values from functions invoked by a DataNode. DataNodes maintain
 * an open server socket so that client code or other DataNodes can
 * read/write data. The host/port for this server is reported to the
 * NameNode, which then sends that information to clients or other
 * DataNodes that might be interested.
 */
public class DataNode implements Runnable {

    private long heartbeatInterval = 3 * 1000;
    private long lastHeartbeatTime = 0;
	private int port = 12345;
	private int nodeID = 0;
	private boolean isRunning = true;
	private Thread dataNodeThread = null;
	private BlockService blockService = null;
	private Thread blockServiceThread = null;
	private ServerSocket dataNodeServerSocket = null;
	private INameSystemService nameNode = null;
	private String nameNodeHostName = null;

	public DataNode() throws Exception {
		dataNodeServerSocket = new ServerSocket(port);
        nameNodeHostName = InetAddress.getLocalHost().getHostAddress();
		blockService = new BlockService(dataNodeServerSocket, true);
		blockServiceThread = new Thread(blockService);
		blockServiceThread.start();
        register();
        dataNodeThread = new Thread(this);
        dataNodeThread.start();
	}

	public static void main(String[] args) {
		try {
			DataNode dataNode = new DataNode();
			dataNode.join();
		} catch (Exception e) {
			System.err.println("Datanode error: ");
			e.printStackTrace();
			System.exit(-1);
		}
	}

	private void register() throws IOException, NotBoundException {
		Registry registry = LocateRegistry.getRegistry();
		nameNode = (INameSystemService) registry.lookup("NameSystemService");
		nodeID = nameNode.register(nameNodeHostName, port, blockService.getAllBlocks());
		System.out.println("Data Node registered. Node ID = " + nodeID);
	}

	/**
	 * Main loop for the DataNode.  Runs until shutdown,
	 * forever calling remote NameNode functions.
	 *
	 * @throws Exception
	 */
	public void offerService() throws Exception {
		while (isRunning) {
            long now = System.currentTimeMillis();
            long waitTime = this.heartbeatInterval - (now - this.lastHeartbeatTime);

            if (waitTime > 0) {
                try {
                    Thread.sleep(waitTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            DataNodeDescriptor thisNode =
                    new DataNodeDescriptor(this.nodeID,
                                           this.nameNodeHostName,
                                           this.port,
                                           this.blockService.getAllBlocks().size());
            LinkedList<DataNodeCommand> commands = nameNode.heartbeat(thisNode);
            processCommands(commands);
            this.lastHeartbeatTime = System.currentTimeMillis();
		}
	}

    /**
     * Process DataNodeCommands from NameNode
     * @param commands
     */
    private void processCommands(List<DataNodeCommand> commands) {
        for (DataNodeCommand command : commands) {

            if (command instanceof DataNodeDeleteCommand) {
                processDeleteCommand((DataNodeDeleteCommand) command);

            } else if (command instanceof DataNodeRestoreCommand) {
                processRestoreCommand((DataNodeRestoreCommand) command);
            }
        }
    }

    private void processDeleteCommand(DataNodeDeleteCommand command) {
        Block block = command.getBlock();
        blockService.deleteBlock(block);
    }

    private void processRestoreCommand(DataNodeRestoreCommand command) {
        Block block = command.getBlock();
        DataNodeDescriptor dataNode = command.getDataNode();
        blockService.copyBlockFromAnotherDataNode(block, dataNode);
    }

	@Override
	public void run() {
		while (isRunning) {
			try {
				offerService();
			} catch (Exception e) {
				e.printStackTrace();
				if (isRunning) {
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e1) {
					}
				}
			}
		}
	}

	void join() {
		if (dataNodeThread != null) {
			try {
				dataNodeThread.join();
                blockService.stop();
			} catch (InterruptedException e) {
			}
		}
	}
}


