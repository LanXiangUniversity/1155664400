import myrmi.registry.LocateRegistry;
import myrmi.registry.Registry;

public class TestClient {

	private TestClient() {
	}

	public static void main(String[] args) {

		String host = (args.length < 1) ? null : args[0];

		try {
			Registry registry = LocateRegistry.getRegistry(host);
			Hello stub1 = (Hello) registry.lookup("HelloService1");
			String response1 = stub1.sayHello("world");
			System.out.println("response1: " + response1);
			Hello stub2 = (Hello) registry.lookup("HelloService2");
			int response2 = stub2.add(stub1);
			System.out.println("response2: " + response2);
		} catch (Exception e) {
			System.err.println("Client exception: " + e.toString());
			e.printStackTrace();
		}
	}
}