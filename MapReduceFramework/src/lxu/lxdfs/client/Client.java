package lxu.lxdfs.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.NotBoundException;

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

		} else if ("write".equals(args[0])) {
            String fileName = args[1];
            String content = args[2];

            ClientOutputStream cos = new ClientOutputStream();
            cos.setFileName(fileName);
            cos.write(content);
        } else if ("read".equals(args[0])) {
            ClientInputStream clientInputStream = new ClientInputStream(args[1]);
            String content = clientInputStream.read();
            System.out.println("Received content = " + content);
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
