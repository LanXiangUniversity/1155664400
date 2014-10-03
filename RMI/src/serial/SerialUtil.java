package serial;

import java.io.*;
import java.util.Objects;

/**
 * Created by Wei on 9/30/14.
 */
public class SerialUtil {
	private ObjectInputStream ois = null;
	private ObjectOutputStream oos = null;

	public Object readObject(InputStream is) throws IOException, ClassNotFoundException {
		ois = new ObjectInputStream(is);
		Object obj = ois.readObject();

		ois.close();

		return obj;
	}

	public void writeObject(Object obj, OutputStream os) throws IOException {
		oos = new ObjectOutputStream(os);
		oos.writeObject(obj);

		oos.close();
	}
}
