package remote;

/**
 * Created by Wei on 10/6/14.
 */
public class MyRemoteException extends Exception {
	private String message;

	@Override
	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public MyRemoteException(String exception) {
		this.message = exception;
	}

	@Override
	public void printStackTrace() {
		System.out.println("Remote Exception: " + this.message);
	}
}
