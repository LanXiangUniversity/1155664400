package lxu.lxdfs.service;

import lxu.lxdfs.client.ClientOutputStream;
import lxu.lxdfs.metadata.*;

import java.nio.file.Path;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Remote Interface for NameSystem
 * Created by Wei on 11/3/14.
 */
public interface INameSystemService extends Remote {
	// Services for Client
	public boolean mkdirs(Path path) throws RemoteException;

	public ClientOutputStream open(Path path) throws RemoteException, NotBoundException;

	public ClientOutputStream create(Path path) throws RemoteException, NotBoundException;

	public boolean delete(Path path) throws RemoteException;

	public boolean exists(Path path) throws RemoteException;

	public LocatedBlock allocateBlock(String fileName, int offset) throws RemoteException;

	public HashSet<DataNodeDescriptor> getBlockLocations(int blockID) throws RemoteException;

	public LocatedBlocks getFileBlocks(String fileName) throws RemoteException;

	public boolean isSafeMode() throws RemoteException;

	public void setSafeMode(boolean isSafeMode) throws RemoteException;

	public void enterSafeMode() throws RemoteException;

	public void exitSafeMode() throws RemoteException;

	// Services for Data Node
	public int register(String dataNodeHostName, int port, ArrayList<Block> blocks) throws RemoteException;

    public LinkedList<DataNodeCommand> heartbeat(DataNodeDescriptor dataNode) throws RemoteException;
}
