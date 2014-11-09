package remote;

import message.RemoteMsg;
import message.RemoteMsgType;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;

/**
 * Created by Wei on 10/5/14.
 */
public class Stub {
	// Send msg to remote host and return its result.
	public RemoteMsg sendMsgToRemoteProxy(RemoteObjectRef ror , RemoteMsg msg) throws IOException,
			ClassNotFoundException {
		// Connect to remote host.
		Socket sock = new Socket(ror.getIPAddress(), ror.getPort());
		ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
		ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());

		// Invoke remote object.
		oos.writeObject(msg);
		RemoteMsg rtnMsg = (RemoteMsg) ois.readObject();

		// Close connections to remote host.
		ois.close();
		oos.close();
		sock.close();

		String exception = rtnMsg.getRemoteException();

		try {
			if (!"null".equals(exception)) {
				throw new MyRemoteException(exception);
			}
		} catch (MyRemoteException e) {
			e.printStackTrace();
		}

		return rtnMsg;
	}

	public RemoteMsg marshall(String methodName, List<Object> params, Class<?>[] paramsType, RemoteObjectRef ror
	) throws NoSuchMethodException {
		RemoteMsg msg = new RemoteMsg(RemoteMsgType.MSG_INVOKE);

		msg.setMethodName(this.getClass().getMethod(methodName, paramsType).getName());

		for (Object obj : params) {
			msg.addParam(obj);
		}

		msg.setContent(ror);

		return msg;
	}

	// Get return value from RemoteMsg.
	public Object unMarshallReturnValue(RemoteMsg msg) {
		return msg.getContent();
	}
}
