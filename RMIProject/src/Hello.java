import myrmi.Remote;

public interface Hello extends Remote{
	public String sayHello(String echo);
	public int getX();
	public int add(Hello another);
}
