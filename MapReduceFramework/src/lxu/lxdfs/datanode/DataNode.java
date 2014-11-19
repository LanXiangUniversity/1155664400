package lxu.lxdfs.datanode;

import lxu.lxdfs.metadata.DataNodeCommand;
import lxu.lxdfs.service.INameSystemService;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.LinkedList;
import java.util.List;

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
		blockService = new BlockService(dataNodeServerSocket);
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
		    /* TODO: send heartbeat */
            LinkedList<DataNodeCommand> commands = nameNode.heartbeat(this.nodeID);
            processCommands(commands);
            this.lastHeartbeatTime = System.currentTimeMillis();
            /* TODO: check newly received block. if true, send block report */
		}
	}

    /**
     * Process DataNodeCommands from NameNode
     * @param commands
     */
    private void processCommands(List<DataNodeCommand> commands) {

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
			} catch (InterruptedException e) {
			}
		}
	}
}


