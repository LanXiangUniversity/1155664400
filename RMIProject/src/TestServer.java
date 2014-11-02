import myrmi.registry.LocateRegistry;
import myrmi.registry.Registry;
import myrmi.server.UnicastRemoteObject;

public class TestServer implements Hello {
	private int x;

	public TestServer(int x) {
		this.x = x;
	}

	public static void main(String[] args) {

		String host = (args.length < 1) ? null : args[0];

		try {
			TestServer server1 = new TestServer(1);
			Hello stub1 = (Hello) UnicastRemoteObject.exportObject(server1);
			Registry registry1 = LocateRegistry.getRegistry(host);
			registry1.bind("HelloService1", stub1);

			TestServer server2 = new TestServer(2);
			Hello stub2 = (Hello) UnicastRemoteObject.exportObject(server2);
			Registry registry2 = LocateRegistry.getRegistry(host);
			registry2.bind("HelloService2", stub2);
			System.out.println("Server started ...");
		} catch (Exception e) {
			System.err.println("Server exception: " + e.toString());
			e.printStackTrace();
		}
	}

	public String sayHello(String echo) {
		x++;
		return "Hello +" + x + " " + echo;
	}

	public int getX() {
		return this.x;
	}

	public int add(Hello another) {
		return this.x + another.getX();
	}
}
