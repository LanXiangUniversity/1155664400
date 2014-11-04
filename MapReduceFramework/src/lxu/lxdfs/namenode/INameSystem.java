package lxu.lxdfs.namenode;

import lxu.lxdfs.Block;
import lxu.lxdfs.BlocksLocation;

import java.nio.file.Path;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * Remote Interface for NameSystem
 * Created by Wei on 11/3/14.
 */
public interface INameSystem extends Remote {
	// Services for Client
	public boolean mkdirs(Path path) throws RemoteException;

	public boolean open(Path path) throws RemoteException;

	public DFSOutputStream create(Path path) throws RemoteException;

	public boolean delete(Path path) throws RemoteException;

	public boolean exists(Path path) throws RemoteException;

	public List<Block> allocateBlock(String fileName, int offset) throws RemoteException;

	public List<BlocksLocation> getBlockLocations(int blockID) throws RemoteException;

	// Services for Data Node

}
