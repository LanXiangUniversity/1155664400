package lxu.lxdfs.service;

import lxu.lxdfs.DataNodeDescriptor;
import lxu.lxdfs.namenode.ClientOutputStream;

import java.nio.file.Path;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

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

	public List<DataNodeDescriptor> allocateBlock(String fileName, int offset) throws RemoteException;

	public List<DataNodeDescriptor> getBlockLocations(int blockID) throws RemoteException;

	// Services for Data Node
    public void register();
}
