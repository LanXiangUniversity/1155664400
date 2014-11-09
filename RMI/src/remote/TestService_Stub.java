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
public class TestService_Stub extends Stub implements TestService {
	private RemoteObjectRef ror;

	public TestService_Stub(RemoteObjectRef ror) {
		this.ror = ror;
	}

	// A stub for remote invocation which packs up method and args into a RemoteMsg
	// and then calls sendMsgRemoteProxy to invoke remote  method.
	@Override
	public String test(String str) throws ClassNotFoundException, NoSuchMethodException, IOException {
		Class<?>[] paramsType = new Class<?>[1];
		List<Object> params = new ArrayList<Object>();

		// Get params and their types.
		paramsType[0] = str.getClass();
		params.add(str);

		RemoteMsg rtnMsg = this.sendMsgToRemoteProxy(this.ror, marshall("test", params, paramsType, this.ror));

		return (String) this.unMarshallReturnValue(rtnMsg);
	}

}
