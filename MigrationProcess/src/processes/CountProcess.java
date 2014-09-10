package processes;

import java.io.Serializable;

/**
 * Created by Wei on 9/6/14.
 */
public class CountProcess implements MigratableProcess {
	private int count = 0;
	private int curCount = 0;

	private volatile boolean suspending;

	public CountProcess(String[] args) {
		this.count = Integer.parseInt(args[0]);
	}

	@Override
	public void run() {
		while(!suspending && curCount < count) {
			curCount++;
			System.out.println("\n[Count Process]: " + curCount);
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		this.suspending = false;
	}

	public void suspend() {
		suspending = true;
		while (suspending) ;
	}
}
