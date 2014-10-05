/**
 * SlaveState.java
 * @author Tong Wei (twei1), Guoli Ma (guolim)
 * 
 * Description: The slave state. Record the slave name, its socket connection,
 * 				and the object input and output stream.
 */

package master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class SlaveState {
    private String slaveName;
    private Socket socket;
    private ObjectInputStream in;
    private ObjectOutputStream out;

    public SlaveState(String slaveName,
            Socket socket,
            ObjectInputStream input,
            ObjectOutputStream output) {
        this.slaveName = slaveName;
        this.socket = socket;
        this.in = input;
        this.out = output;
    }

    public String getSlaveName() {
        return this.slaveName;
    }

    public Socket getSocket() {
        return this.socket;
    }
    public ObjectInputStream getOIS() {
        return in;
    }

    public ObjectOutputStream getOOS() {
        return out;
    }

    public void setSlaveName(String name) {
        this.slaveName = name;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }
    public void setOIS(ObjectInputStream ois) {
        this.in = ois;
    }

    public void setOOS(ObjectOutputStream oos) {
        out = oos;
    }

    
    public void deleteSlave() {
    	try {
			this.in.close();
			this.out.close();
			this.socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}
