package remote;

import message.RemoteMsg;
import message.RemoteMsgType;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Wei on 10/4/14.
 */
public class TestService_stub implements TestService {
	private RemoteObjectRef ror;

	public TestService_stub(RemoteObjectRef ror) {
		this.ror = ror;
	}

	// Send msg to remote host and return its result.
	public RemoteMsg sendMsgToRemoteProxy(RemoteMsg msg) throws IOException, ClassNotFoundException {
		// Connect to remote host.
		System.out.println(this.ror.getIPAddress());
		System.out.println(this.ror.getPort());
		Socket sock = new Socket(this.ror.getIPAddress(), this.ror.getPort());
		System.out.println("Connect to remote host.");
		System.out.println("service name" + ((RemoteObjectRef)msg.getContent()).getRemoteInterfaceName());
		ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
		ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());

		// Invoke remote object.
		System.out.println("Write remote msg.");
		oos.writeObject(msg);
		System.out.println("Invoking remote method.");
		RemoteMsg rtnMsg = (RemoteMsg) ois.readObject();
		System.out.println("Return from remote method.");

		// Close connections to remote host.
		ois.close();
		oos.close();
		sock.close();

		return rtnMsg;
	}

	// A stub for remote invocation which packs up method and args into a RemoteMsg
	// and then calls sendMsgRemoteProxy to invoke remote  method.
	public String test(String str) throws ClassNotFoundException, NoSuchMethodException, IOException {
		Class<?>[] paramsType = new Class<?>[1];
		List<Object> params = new ArrayList<Object>();

		// Get params and their types.
		paramsType[0] = str.getClass();
		params.add(str);

		RemoteMsg rtnMsg = this.sendMsgToRemoteProxy(marshall("test", params, paramsType));

		return (String) this.unMarshallReturnValue(rtnMsg);
	}

	// Get return value from RemoteMsg.
	private Object unMarshallReturnValue(RemoteMsg msg) {
		return msg.getContent();
	}

	// Marshall
	private RemoteMsg marshall(String methodName, List<Object> params, Class<?>[] paramsType) throws NoSuchMethodException {
		RemoteMsg msg = new RemoteMsg(RemoteMsgType.MSG_INVOKE);

		msg.setMethodName(this.getClass().getMethod(methodName, paramsType).getName());

		for (Object obj : params) {
			msg.addParam(obj);
		}

		msg.setContent(this.ror);

		return msg;
	}
}
