package lxu.lxdfs.client;

import java.io.*;
import java.rmi.NotBoundException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Wei on 11/8/14.
 */
public class Client {
	private ClientState state;
	private BufferedReader consoleReader;

	public Client() {
		this.state = ClientState.RUNNING;
		this.consoleReader = new BufferedReader(
				new InputStreamReader(System.in));
	}

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

		String[] args = cmd.split(" ");

		if ("ls".equals(args[0])) {

		} else if ("mkdir".equals(args[0])) {

		} else if ("touch".equals(args[0])) {

		} else if ("rm".equals(args[0])) {

		} else if ("put".equals(args[0])) {
			//String fileName = args[1];
			//String content = args[2];
            String localFileName = args[1];
            String dfsFileName = args[2];

            List<String> content = new LinkedList<>();

            BufferedReader reader = new BufferedReader(new FileReader(localFileName));
            String line = null;
            while ((line = reader.readLine()) != null) {
                content.add(line);
            }

			ClientOutputStream cos = new ClientOutputStream();
			//cos.setFileName(fileName);
            cos.setFileName(dfsFileName);
			cos.write(content);
		} else if ("get".equals(args[0])) {
			ClientInputStream clientInputStream = new ClientInputStream(args[1]);
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
		System.out.println("Usage:");
		System.out.println("ls");
		System.out.println("mkdir");
		System.out.println("touch");
		System.out.println("rm");
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
