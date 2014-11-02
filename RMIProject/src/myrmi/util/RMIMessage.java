package myrmi.util;


/**
 * RMIMessage.java
 * 
 * @author Tong Wei, Guoli Ma {twei1, guolim}@andrew.cmu.edu
 * 
 * This class marshelles all needed message for communication.
 */

import java.io.Serializable;

public class RMIMessage implements Serializable {
	
    /**
	 * 
	 */
	private static final long serialVersionUID = 1125783864957569936L;

	public static enum MessageType {
        REMOTE_CALL, 
        LOOK_UP, BIND, UNBIND, REBIND, LIST,
        GET_REGISTRY, 
        RESPONSE
    }

	/* For method name, reference name, or response name */
	private String name;
    private Object[] args;
    private MessageType messageType;
    /* Carrier for ObjID, return value */
    private Object carrier;

    /* ---------------- Constructor ---------------- */
    public RMIMessage() { 
        super();
    }
    
    public static boolean checkResponse(RMIMessage returnMessage) {
    	if (returnMessage.getMessageType() == RMIMessage.MessageType.RESPONSE) {
        	if (returnMessage.getName().equals("OK")) {
        		return true;
        	} else {
        		System.out.println(returnMessage.getName());
        	}
        } else {
        	System.out.println("RMIRegistry returned non-response message");
        }
    	return false;
    }

    /* -------------------- set -------------------- */
    public void setName(String name) {
        this.name = name;
    }

    public void setArgs(Object[] args) {
        this.args = args;
    }

    public void setMessageType(MessageType type) {
        this.messageType = type;
    }

    public void setCarrier(Object obj) {
        this.carrier = obj;
    }
    
    /* -------------------- get -------------------- */
    public String getName() {
        return this.name;
    }

    public Object[] getArgs() {
        return this.args;
    }

    public MessageType getMessageType() {
        return this.messageType;
    }

    public Object getCarrier() {
        return this.carrier;
    }

    /* -------------------- with -------------------- */
    public RMIMessage withName(String name) {
        this.name = name;
        return this;
    }

    public RMIMessage withArgs(Object[] args) {
        this.args = args;
        return this;
    }

    public RMIMessage withMessageType(MessageType type) {
        this.messageType = type;
        return this;
    }

    public RMIMessage withCarrier(Object obj) {
        this.carrier = obj;
        return this;
    }
}
