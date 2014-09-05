package tcpThread;

import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by Wei on 9/5/14.
 */
public class TcpServerThread implements Runnable {
	private int port;

	public TcpServerThread(int port) {
		this.port = port;
	}

	@Override
	public void run() {
		try {
			ServerSocket srvSock = new ServerSocket(port);

			while (true) {
				Socket req = srvSock.accept();

				TcpClientThread clientThread = new TcpClientThread(req);
				new Thread(clientThread).start();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
