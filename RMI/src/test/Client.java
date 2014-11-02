package test;

import org.jetbrains.annotations.NotNull;
import registry.FileRegistry;
import remote.RemoteObjectRef;
import remote.TestService;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by Wei on 10/3/14.
 */
public class Client {
	public static final String SERVER_IP = "localhost";
	public static final int SERVER_PORT = 9901;
	@NotNull
	public static String REG_PATH = "/Users/parasitew/Documents/CMU/15640/lab/lab2/registry/reg.dat";

	public static void main(String[] args) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
		String serviceName = "testService";
		FileRegistry registry = new FileRegistry(REG_PATH);
		RemoteObjectRef ror = registry.lookup(serviceName);

		assert ror != null;
		TestService srv = (TestService) ror.localise();

		assert srv != null;
		System.out.println("Result: " + srv.test("testService"));
	}
}
