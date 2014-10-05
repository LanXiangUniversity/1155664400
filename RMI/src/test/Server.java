package test;

import message.RemoteMsg;
import message.RemoteMsgType;
import org.jetbrains.annotations.NotNull;
import registry.FileRegistry;
import remote.RORTable;
import remote.RemoteObjectRef;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by Wei on 10/3/14.
 */
public class Server {
	public static final int LISTEN_PORT = 9901;
	@NotNull
	public static String REG_PATH = "/Users/parasitew/Documents/CMU/15640/lab/lab2/registry/reg.dat";
	@NotNull
	private String IPAddress = "localhost";
	private int dispatcherPort = 12345;
	private boolean isRunning;
	private Dispatcher dispatcher;
	private RORTable rorTbl;

	public Server() {
		this.dispatcher = new Dispatcher();
		this.isRunning = true;
		this.rorTbl = new RORTable();
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
		Server srv = new Server();
		srv.registService();
		srv.startDispatcher();
	}

	public void registService() throws ClassNotFoundException, IllegalAccessException, InstantiationException, IOException {
		int counter = 0;
		FileRegistry registry = new FileRegistry(REG_PATH);
		String serviceName = "remote.TestService";

		// Create a remote object.
		Class<?> c = Class.forName(serviceName + "Impl");
		Object obj = c.newInstance();
		this.rorTbl.addObject(counter, obj);
		// Register a remote service.
		RemoteObjectRef ror = new RemoteObjectRef("localhost", 12345, counter, serviceName);
		registry.rebind("testService", ror);
		counter++;
	}

	public void startDispatcher() {
		new Thread(this.dispatcher).start();
	}

	// Parse remoteMsg, return result by calling local methods.
	@NotNull
	public RemoteMsg dispatch(@NotNull RemoteMsg msg) throws ClassNotFoundException, IllegalAccessException, InstantiationException, InvocationTargetException {
		RemoteMsg rtnMsg = new RemoteMsg();

		if (msg.getMsgType() == RemoteMsgType.MSG_INVOKE) {
			System.out.println("Invoking method.....");

			// The method to invoke.
			String methodName = msg.getMethodName();

			RemoteObjectRef ror = (RemoteObjectRef) msg.getContent();
			Object obj = this.rorTbl.findObject(ror.getObjKey());

			Class<?> c = Class.forName(ror.getRemoteInterfaceName() + "Impl");
			Method[] methods = c.getDeclaredMethods();
			Method method = null;

			for (Method m : methods) {

				if (m.getName().equals(msg.getMethodName()) && msg.getParams().size() ==
						m.getParameterTypes().length) {
					method = m;
					System.out.println("Find method " + method.getName());
				}
			}

			assert method != null;
			Object[] params = new Object[msg.getParams().size()];

			for (int i = 0; i < msg.getParams().size(); i++) {
				params[i] = msg.getParams().get(i);
			}
			Object rtn = method.invoke(obj, params);

			rtnMsg = marshallReturnValue(rtn);

		} else {
			rtnMsg.setMsgType(RemoteMsgType.MSG_ERROR);
			System.out.println("Remote invocation error.");
		}

		System.out.println("return value: " + (String) rtnMsg.getContent());

		return rtnMsg;
	}

	@NotNull
	public RemoteMsg marshallReturnValue(Object obj) {
		RemoteMsg rtnMsg = new RemoteMsg(RemoteMsgType.MSG_RETURN);
		rtnMsg.setContent(obj);

		return rtnMsg;
	}

	// A proxy dispatcher that listens for the remote invocation from stub, dispatch it
	// to some local method and returns the result.
	class Dispatcher implements Runnable {

		@Override
		public void run() {
			try {
				ServerSocket srvSock = new ServerSocket(dispatcherPort);
				while (isRunning) {
					System.out.println("dispatcher is listening.");
					Socket reqSock = srvSock.accept();

					System.out.println("Receive remote connection.");
					ObjectInputStream ois = new ObjectInputStream(reqSock.getInputStream());
					ObjectOutputStream oos = new ObjectOutputStream(reqSock.getOutputStream());

					RemoteMsg msg = (RemoteMsg) ois.readObject();
					System.out.println("Read remote msg.");
					RemoteMsg rtnMsg = dispatch(msg);
					System.out.println("Write remote msg.");
					oos.writeObject(rtnMsg);

					System.out.println("Send return value back to remote.");
					oos.close();
					ois.close();
					reqSock.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
	}

}