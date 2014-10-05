package remote;

import message.RemoteMsg;
import message.RemoteMsgType;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Wei on 10/4/14.
 */
public class TestService_stub implements TestService, Stub {
	private RemoteObjectRef ror;

	public TestService_stub(RemoteObjectRef ror) {
		this.ror = ror;
	}

	// Send msg to remote host and return its result.
	@NotNull
	@Override
	public RemoteMsg sendMsgToRemoteProxy(RemoteMsg msg) throws IOException, ClassNotFoundException {
		// Connect to remote host.
		Socket sock = new Socket(this.ror.getIPAddress(), this.ror.getPort());
		ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
		ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());

		// Invoke remote object.
		oos.writeObject(msg);
		RemoteMsg rtnMsg = (RemoteMsg) ois.readObject();

		// Close connections to remote host.
		ois.close();
		oos.close();
		sock.close();

		return rtnMsg;
	}

	// A stub for remote invocation which packs up method and args into a RemoteMsg
	// and then calls sendMsgRemoteProxy to invoke remote  method.
	@NotNull
	@Override
	public String test(@NotNull String str) throws ClassNotFoundException, NoSuchMethodException, IOException {
		Class<?>[] paramsType = new Class<?>[1];
		List<Object> params = new ArrayList<Object>();

		// Get params and their types.
		paramsType[0] = str.getClass();
		params.add(str);

		RemoteMsg rtnMsg = this.sendMsgToRemoteProxy(marshall("test", params, paramsType, this.ror));

		return (String) this.unMarshallReturnValue(rtnMsg);
	}

	// Get return value from RemoteMsg.
	@Override
	public Object unMarshallReturnValue(@NotNull RemoteMsg msg) {
		return msg.getContent();
	}

	// Marshall
	@NotNull
	@Override
	public RemoteMsg marshall(String methodName, @NotNull List<Object> params, Class<?>[] paramsType, RemoteObjectRef ror
	) throws NoSuchMethodException {
		RemoteMsg msg = new RemoteMsg(RemoteMsgType.MSG_INVOKE);

		msg.setMethodName(this.getClass().getMethod(methodName, paramsType).getName());

		for (Object obj : params) {
			msg.addParam(obj);
		}

		msg.setContent(ror);

		return msg;
	}
}
