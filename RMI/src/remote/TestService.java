package remote;

import java.io.IOException;

/**
 * Created by Wei on 10/4/14.
 */
public interface TestService extends RemoteInterface {
	public String test(String str) throws ClassNotFoundException, NoSuchMethodException, IOException;
}
