package lxu.lxdfs.client;

import lxu.lxdfs.datanode.DataNodePacket;
import lxu.lxdfs.metadata.Block;
import lxu.lxdfs.metadata.DataNodeDescriptor;
import lxu.lxdfs.metadata.LocatedBlock;
import lxu.lxdfs.service.INameSystemService;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Created by Wei on 11/8/14.
 */
public class ClientInputStream extends ClientStream {
    private String fileName;
    private INameSystemService nameSystemService;

    public ClientInputStream(String fileName, String masterAddr, int rmiPort)
            throws RemoteException, NotBoundException {
        this.fileName = fileName;
        Registry registry = LocateRegistry.getRegistry(masterAddr, rmiPort);
        this.nameSystemService = (INameSystemService) registry.lookup("NameSystemService");
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public INameSystemService getNameSystemService() {
        return nameSystemService;
    }

    public void setNameSystemService(INameSystemService nameSystemService) {
        this.nameSystemService = nameSystemService;
    }

    // Read the content of file.
    public String read() throws IOException, ClassNotFoundException {
        // Get  AllocatedBlocks of the file from NameNode.
        LocatedBlock[] blockToDataNodeMap = null;
        blockToDataNodeMap = this.nameSystemService.getFileBlocks(this.fileName).getBlocks();
        String res = "";

        // Get the content of each block sequentially.
        for (LocatedBlock locatedBlock : blockToDataNodeMap) {
            Block block = locatedBlock.getBlock();

            Socket sock = null;
            ObjectOutputStream oos = null;
            ObjectInputStream ois = null;

            HashSet<DataNodeDescriptor> locations = locatedBlock.getLocations();
            Iterator<DataNodeDescriptor> iterator = locations.iterator();
            DataNodeDescriptor dataNodeDescriptor = iterator.next();

            // Connect to the first DataNode.
            sock = new Socket(dataNodeDescriptor.getDataNodeIP(),
                    dataNodeDescriptor.getDataNodePort());

            // Read a Block from DataNode
            oos = new ObjectOutputStream(sock.getOutputStream());

            oos.writeObject(generateReadPacket(block));
            System.out.println("here");

            ois = new ObjectInputStream(sock.getInputStream());
            DataNodePacket packet = (DataNodePacket) ois.readObject();
            ois.close();
            sock.close();

            ArrayList<String> lines = packet.getLines();

            for (String line : lines) {
                res += "\n" + line;
            }
        }

        return res.substring(1);
    }


    public ClientPacket generateReadPacket(Block block) {
        ClientPacket packet = new ClientPacket();
        packet.setOperation(ClientPacket.BLOCK_READ);
        packet.setBlock(block);
        return packet;
    }
}
