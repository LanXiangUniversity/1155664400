package message;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Wei on 10/4/14.
 */
public class RemoteMsg implements Serializable {
	private RemoteMsgType msgType;
	//private Method method;
	private String methodName;
	private List<Object> params;
	private Object content;
	private String remoteException;

	public RemoteMsg(RemoteMsgType msgType) {
		this.msgType = msgType;
		this.params = new ArrayList<Object>();

	}

	public String getRemoteException() {
		return remoteException;
	}

	public void setRemoteException(String remoteException) {
		this.remoteException = remoteException;
	}

	public RemoteMsg() {
	}

	public Object getContent() {
		return content;
	}

	public void setContent(Object content) {
		this.content = content;
	}

	public void addParam(Object arg) {
		this.params.add(arg);
	}

	public RemoteMsgType getMsgType() {
		return msgType;
	}

	public void setMsgType(RemoteMsgType msgType) {
		this.msgType = msgType;
	}

	public String getMethodName() {
		return methodName;
	}

	public void setMethodName(String methodName) {
		this.methodName = methodName;
	}

	public List<Object> getParams() {
		return params;
	}
}
