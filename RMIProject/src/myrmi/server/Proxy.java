package myrmi.server;


/**
 * Proxy.java
 * 
 * @author Tong Wei, Guoli Ma {twei1, guolim}@andrew.cmu.edu
 * 
 * A proxy dispatcher for remote call. It receive remote call message, search
 * remote object, and invoke the specified method.
 */


import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import myrmi.Remote;
import myrmi.util.Communication;
import myrmi.util.RMIMessage;


/**
 * Proxy
 * 
 * It is just a thread running on server that receive remote call message. All
 * of its methods are static. The thread is created by a static block so server
 * (user) don't have to explicitly create the proxy thread.
 * In addition, the proxy helps to create the stub that is exported by the 
 * UnicastRemoteObject class.
 */
public final class Proxy {
	public static int PROXY_PORT = 15440;
    private static ServerSocket serverSocket;
    private static String host;
	
    private static ConcurrentHashMap<String, Remote> objectMap =
            new ConcurrentHashMap<String, Remote>();

    /* static block, create the running thread */
    static {
        try {
            serverSocket = new ServerSocket(PROXY_PORT);
            host = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            System.out.println("Proxy: Cannot get host address");
        } catch (IOException e) {
            System.out.println("Proxy: Cannot create proxy ServerSocket");
            e.printStackTrace();
        }
    	Thread skeletonThread = new Thread(new SkeletonThread());
    	skeletonThread.start();
    }

    /**
     * SkeletonThread
     * 
     * A thread that receives all remote call message, unmarshells the message,
     * and invokes the specified method.
     */
    private static class SkeletonThread implements Runnable {
    	/**
    	 * invoke
    	 * Invoke the specified method of the specified remote object.
    	 * @param obj - Remote referenced object.
    	 * @param methodName - The specified method name.
    	 * @param args - Method arguments.
    	 * @return The return value of the method.
    	 * 
    	 * @throws NoSuchMethodException
    	 * @throws SecurityException
    	 * @throws IllegalAccessException
    	 * @throws IllegalArgumentException
    	 * @throws InvocationTargetException
    	 */
        private Object invoke(Remote obj, String methodName, Object[] args) 
        	throws NoSuchMethodException, SecurityException, 
        		   IllegalAccessException, IllegalArgumentException, 
        		   InvocationTargetException
        {
        	Class<?> objClass = obj.getClass();
        	
        	/* Get the types of all arguments */
        	Class<?>[] argTypes = new Class<?>[args.length];
        	
        	for (int j = 0; j < args.length; ++j) {
        		argTypes[j] = args[j].getClass();
        		
        		/* Whether the argument is a remote stub */
        		Class<?> i = getImplementedRemoteInterface(argTypes[j]);
        		
        		if (i != null) { /* The argument is a remote stub */
        			/* Get the remote object it referenced */
        			RemoteRef remoteObjectRef = 
        					((RemoteStub)args[j]).getRemoteRef();
        			
        			/* stub reference for object in this proxy */
        			if (remoteObjectRef.getHost().equals(host) &&
        					remoteObjectRef.getPort() == PROXY_PORT) {
        				/* Replace this argument with that it references to*/
        				String objID = remoteObjectRef.getObjID();
        				
        				Remote referencedObj = objectMap.get(objID);
        				args[j] = referencedObj;
        				
                		argTypes[j] = 
                				getImplementedRemoteInterface(args[j].getClass());
        			} else {
        				/* stub reference for object in other proxy */
        				argTypes[j] = i;
        			}
        		}
        	}
        	
        	System.out.print("Remote call received " + methodName + "(");
        	for (Class<?> i : argTypes) {
        		System.out.print(i.getName());
        	}System.out.println(")");
        	
        	/* Invoke the method */
        	Method method = objClass.getMethod(methodName, argTypes);
        	Object returnValue = method.invoke(obj, args);
        	
            return returnValue;
        }

        /**
         * run
         * 
         * Main thread of Proxy. It receive remote all message, unmarshell the
         * message and call invoke.
         */
        public void run() {
        	while (true) {
	            try {
	                Socket socket = serverSocket.accept();
	                
	                RMIMessage message = Communication.receiveMessage(socket);
	                
	                switch (message.getMessageType()) {
	                    case REMOTE_CALL:
	                    	/* Unmarshell message */
	                    	String methodName = message.getName();
	                    	Object[] args = message.getArgs();
	                    	String objID = (String) message.getCarrier();
	                    	Remote obj = objectMap.get(objID);
	                    	
	                    	Object returnValue = null;
	                    	String responseName = "OK";

	            			try {
	            				returnValue = invoke(obj, methodName, args);
	            			} catch (NoSuchMethodException | 
	            					 SecurityException e) {
	            				responseName = obj.getClass().getName() + 
	            							   " NoSuchMethodException |" +
	            							   " SecurityException";
	            				e.printStackTrace();
	            			} catch (IllegalAccessException |
	            					 IllegalArgumentException | 
	            					 InvocationTargetException e) {
	            				responseName = obj.getClass().getName() + 
	     							   " IllegalAccessException |" + 
	            					   " IllegalArgumentException |" +
	     							   " InvocationTargetException";
	            				e.printStackTrace();
	            			}
	                    	
	                    	RMIMessage response = 
	                    			new RMIMessage()
	                    			  	.withMessageType(RMIMessage.MessageType.RESPONSE)
	                    			  	.withName(responseName)
	                    			  	.withCarrier(returnValue);
	                    	
	                    	Communication.sendMessage(response, socket);
	                        break;
	                    default:
	                        System.out.println("Proxy received wrong message");
	                        break;
	                }
	            } catch (IOException e) {
	            	e.printStackTrace();
	            }
        	}
        }
    }
    
    /**
     * generateObjectID - Generate a unique ID of each remote object.
     * @return A unique ID.
     */
    private static String generateObjectID() {
        return UUID.randomUUID().toString();
    }

    /**
     * getImplementedRemoteInterface - Get which remote interface a remote 
     * 								   object implements.
     * @param remoteClass - The class of the remote object.
     * @return The remote interface implemented.
     */
    private static Class<?> getImplementedRemoteInterface(Class<?> remoteClass) {
        while (remoteClass != null) {
        	for (Class<?> i : remoteClass.getInterfaces()) {
        		if (Remote.class.isAssignableFrom(i)) {
                    return i;  // this remoteClass implements remote interface
        		}
        	}
            remoteClass = remoteClass.getSuperclass();
        }
        return null;
    }

    /**
     * generateStub - Given a remote object, generate its stub.
     * @param impl - A remote object
     * @return A stub
     * @throws ClassNotFoundException
     */
	public static Remote generateStub(Remote impl)
            throws ClassNotFoundException
    {
        Class<?> implClass = impl.getClass();

        if (getImplementedRemoteInterface(implClass) == null) {
            throw new ClassNotFoundException(
                    "class does not implement myrmi.Remote");
        }

        String stubName = implClass.getName() + "_stub";
        Remote stubInstance = null;

        /* Use reflection to create the stub object */
        try {
            String objID = generateObjectID();
            RemoteRef remoteRef = new RemoteRef(objID, host, PROXY_PORT);
        
            Class<?> stubClass = Class.forName(stubName);
            Class<?> remoteRefClass = remoteRef.getClass();
            Constructor<?> cons = stubClass.getConstructor(remoteRefClass);
            stubInstance = (Remote)cons.newInstance(remoteRef);

            objectMap.put(objID, impl);
        } catch (ClassNotFoundException e) {
            System.out.println("Stub class not found: " + stubName);
        } catch (NoSuchMethodException e) {
            System.out.println("Stub class missing constructor" + stubName);
        } catch (InstantiationException e) {
            System.out.println("Can't create instance of stub class: " + stubName);
        } catch (IllegalAccessException e) {
            System.out.println("Stub class constructor not public: " + stubName);
        } catch (InvocationTargetException e) {
            System.out.println("Exception creating instance of stub class: " + stubName);
        }

        return stubInstance;
	}
}
