package remote;

import message.RemoteMsg;

import java.io.IOException;
import java.util.List;

/**
 * Created by Wei on 10/5/14.
 */
public interface Stub {
	public RemoteMsg sendMsgToRemoteProxy(RemoteMsg msg) throws IOException, ClassNotFoundException;
	public RemoteMsg marshall(String methodName, List<Object> params, Class<?>[] paramsType) throws NoSuchMethodException;
	public Object unMarshallReturnValue(RemoteMsg msg);
}
