package rmi;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Created by Wei on 10/3/14.
 */
public class LocateSimpleRegistry {
	public static SimpleRegistry getRegistry(String host, int port) {
		// Open socket.
		try {
			Socket sock = new Socket(host, port);

			// get TCP streams and wrap them.
			BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
			PrintWriter out = new PrintWriter(sock.getOutputStream(), true);

			// Ask.
			out.println("Who ar you?");

			// Get answer.
			if ("I am a simple registry".equals(in.readLine())) {
				return new SimpleRegistry(host, port);
			} else {
				System.out.println("Somebody is there but not a registry");
				return null;
			}
		} catch (Exception e) {
			System.out.println("Nobody is there");
			e.printStackTrace();
			return null;
		}
	}
}
