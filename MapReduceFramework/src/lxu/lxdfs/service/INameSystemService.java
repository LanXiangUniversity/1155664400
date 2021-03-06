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
 * INameSystemService
 * Created by Wei on 11/3/14.
 *
 * Remote Interface for NameSystem
 */
public interface INameSystemService extends Remote {
    public ClientOutputStream open(Path path) throws RemoteException, NotBoundException;

    public void create(String fileName) throws RemoteException, NotBoundException;

    public boolean delete(String string) throws RemoteException;

    public boolean exists(String string) throws RemoteException;

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

    public HashSet<String> ls() throws RemoteException;
}
