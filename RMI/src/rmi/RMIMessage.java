package rmi;

import java.io.Serializable;

/**
 * Created by Wei on 10/3/14.
 */
public class RMIMessage implements Serializable {
	private String msg = "This is an RMIMessage.";
	private String t = "";

	public RMIMessage(String t) {
		this.t = t;
	}

	public void invoke() {
		System.out.println(msg);
		System.out.println(t);
	}
}
