package lxu.lxdfs.service;

import lxu.lxdfs.client.ClientOutputStream;
import lxu.lxdfs.metadata.AllocatedBlock;
import lxu.lxdfs.metadata.Block;
import lxu.lxdfs.metadata.DataNodeDescriptor;

import java.nio.file.Path;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Remote Interface for NameSystem
 * Created by Wei on 11/3/14.
 */
public interface INameSystemService extends Remote {
	// Services for Client
	public boolean mkdirs(Path path) throws RemoteException;

	public ClientOutputStream open(Path path) throws RemoteException;

	public ClientOutputStream create(Path path) throws RemoteException;

	public boolean delete(Path path) throws RemoteException;

	public boolean exists(Path path) throws RemoteException;

	public AllocatedBlock allocateBlock(String fileName, int offset) throws RemoteException;

	public HashSet<DataNodeDescriptor> getBlockLocations(int blockID) throws RemoteException;

	public ArrayList<AllocatedBlock> getFileBlocks(String fileName) throws RemoteException;

	// Services for Data Node
    public int register(String dataNodeHostName, int port, ArrayList<Block> blocks);
}
