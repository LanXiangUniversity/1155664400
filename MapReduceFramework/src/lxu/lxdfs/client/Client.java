package lxu.lxdfs.client;

import lxu.lxdfs.service.INameSystemService;
import lxu.lxmapreduce.configuration.Configuration;

import java.io.*;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Client.java
 * Created by Wei on 11/8/14.
 *
 * This is the command line client to interact with lxdfs.
 * Supported command:
 * ls                            print all files stored in dfs
 * rm <file>                     delete a file in dfs
 * get <dfs file> <local file>   get a dfs file to local file system
 * put <local file> <dfs file>   put a local file to dfs
 */
public class Client {
    private ClientState state;
    private BufferedReader consoleReader;
    private INameSystemService nameSystemService;
    private Configuration conf;

    /**
     * Constructor
     *
     * Connecting to {@link lxu.lxdfs.namenode.NameNode} using java rmi
     *
     * @throws RemoteException
     * @throws NotBoundException
     */
    public Client() throws RemoteException, NotBoundException {
        conf = new Configuration();
        String masterAddr = conf.getSocketAddr("master.address", "localhost");
        int rmiPort = conf.getInt("rmi.port", 1099);
        Registry registry = LocateRegistry.getRegistry(masterAddr, rmiPort);
        this.nameSystemService = (INameSystemService) registry.lookup("NameSystemService");
        this.state = ClientState.RUNNING;
        this.consoleReader = new BufferedReader(
                new InputStreamReader(System.in));
    }

    /**
     * main
     *
     * print prompt and parse input
     *
     * @param args
     * @throws IOException
     * @throws NotBoundException
     * @throws ClassNotFoundException
     */
    public static void main(String[] args) throws IOException, NotBoundException, ClassNotFoundException {
        Client client = new Client();

        while (client.getState() == ClientState.RUNNING) {
            client.printPompt();
            client.parseInput();
        }

        client.exit();
    }

    /**
     * Show cmd pompt.
     */
    public void printPompt() {
        System.out.print("lxdfs $ ");
    }

    /**
     * Parse user input and execute DFS operations.
     *
     * @throws IOException
     */
    public void parseInput() throws IOException, NotBoundException, ClassNotFoundException {
        String cmd = this.consoleReader.readLine();
        if ("".equals(cmd)) {
            return;
        } else if (cmd == null) {
            this.state = ClientState.TERMINATED;
            return;
        }

        String[] args = cmd.split(" ");

        if ("ls".equals(args[0])) {
            Set<String> files = this.nameSystemService.ls();

            for (String file : files) {
                System.out.println(file);
            }
        } else if ("rm".equals(args[0])) {
            if (!this.nameSystemService.exists(args[1])) {
                System.out.println("No such file.");
                return;
            }

            this.nameSystemService.delete(args[1]);
        } else if ("put".equals(args[0])) {
            String localFileName = args[1];
            String dfsFileName = args[2];

            if (this.nameSystemService.exists(args[2])) {
                System.out.println("File exists");
                return;
            }

            List<String> content = new LinkedList<>();

            BufferedReader reader = new BufferedReader(new FileReader(localFileName));
            String line = null;
            while ((line = reader.readLine()) != null) {
                content.add(line);
            }

            String masterAddr = conf.getSocketAddr("master.address", "localhost");
            int rmiPort = conf.getInt("rmi.port", 1099);
            ClientOutputStream cos = new ClientOutputStream(masterAddr, rmiPort);
            cos.setFileName(dfsFileName);
            cos.write(content);

        } else if ("get".equals(args[0])) {
            if (!this.nameSystemService.exists(args[1])) {
                System.out.println("No such file.");
                return;
            }

            String masterAddr = conf.getSocketAddr("master.address", "localhost");
            int rmiPort = conf.getInt("rmi.port", 1099);
            ClientInputStream clientInputStream = new ClientInputStream(args[1], masterAddr, rmiPort);
            String content = clientInputStream.read();

            File file = new File(args[2]);
            PrintWriter pw = new PrintWriter(new FileWriter(file));
            pw.println(content);

            pw.close();
        } else {
            this.showHelpInfo(args[0]);
        }
    }

    public void showHelpInfo(String opt) {
        System.out.println("Unkonwn option: " + opt);
        System.out.println("Usage: (Waring: Current version does not support directory and wild card)");
        System.out.println("ls                            print all files stored in dfs");
        System.out.println("rm <file>                     delete a file in dfs");
        System.out.println("get <dfs file> <local file>   get a dfs file to local file system");
        System.out.println("put <local file> <dfs file>   put a local file to dfs");
    }

    public void exit() throws IOException {
        this.consoleReader.close();
    }

    public ClientState getState() {
        return state;
    }

    public void setState(ClientState state) {
        this.state = state;
    }
}
