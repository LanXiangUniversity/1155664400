package myrmi.registry;

/**
 * LocateRegistry.java
 * 
 * @author Tong Wei, Guoli Ma {twei1, guolim}@andrew.cmu.edu
 * 
 * A simulation for java.rmi.registry.LocateRegistry.
 * LocateRegistry is used to obtain a reference to a remote object registry on 
 * a particular host.
 */

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

import myrmi.util.RMIMessage;
import myrmi.util.Communication;

public final class LocateRegistry {
	
	/* Private constructor to disable public construction. */
    private LocateRegistry() {}

    /**
     * getRegistry - Returns a reference to the remote object Registry for the
     * 				 local host on the default registry port of 15640. 
     */
    public static Registry getRegistry() {
        return getRegistry(null, Registry.REGISTRY_PORT);
    }

    /**
     * getRegistry - Returns a reference to the remote object Registry for the 
     * 				 local host on the specified port. 
     */
    public static Registry getRegistry(int port) {
        return getRegistry(null, port);
    }

    /**
     * getRegistry - Returns a reference to the remote object Registry on the 
     * 				 specified host on the default registry port of 15640. If
     * 				 host is null, the localhost is used.
     */
    public static Registry getRegistry(String host) {
        return getRegistry(host, Registry.REGISTRY_PORT);
    }

    /**
     * getRegistry - Returns a locally created remote reference to the remote 
     * 				 object Registry on the specified host and port.
     */
    public static Registry getRegistry(String host, int port) {
        Registry registry = null;
        
        /* Check whether to use local host */
        if (host == null || host.equals("")) {
            try {
                host = InetAddress.getLocalHost().getHostAddress();
            } catch(Exception e){
                host = "";
            }
        }

        /* Send get registry message to RMIRegestry */
        try {
            Socket socket = new Socket(host, port);
            System.out.println("Get registry from RMIRegistry: " + 
            				   host + ":" + port);
            RMIMessage message = 
                    new RMIMessage()
                        .withMessageType(RMIMessage.MessageType.GET_REGISTRY);
            
            if (Communication.sendMessage(message, socket) == true) {
                RMIMessage returnMessage = 
                        Communication.receiveMessage(socket);
                if (RMIMessage.checkResponse(returnMessage)) {
                    registry = (Registry)returnMessage.getCarrier();
                }
            } else {
                System.exit(-1);
            }
        } catch (IOException e) {
            System.out.println("LocateRegistry: cannot connect to RMIRegistry");
            e.printStackTrace();
            System.exit(-1);
        }

        return registry;
    }
}
