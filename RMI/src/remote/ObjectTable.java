package remote;


import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by Wei on 10/3/14.
 */
public class ObjectTable implements Serializable {
	private int count;
	private HashMap entries;

	public ObjectTable() {
		entries = new HashMap();
	}

	public void addObject(int objKey, Object obj) {
		entries.put(objKey, obj);

	}

	public void addObject(Object obj, String host, int port) {
		//entries.put(this.count, obj);

	}

	public Object findObject(int objKey) {
		if (this.entries.containsKey(objKey)) {
			System.out.println("Find " + objKey + " in ROR table.");
			return entries.get(objKey);
		} else {
			System.out.println("Can't find this remote object.");
		}

		return null;
	}
}
