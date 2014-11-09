/**
 * SqrtProcess.java
 * @author Tong Wei (twei1), Guoli Ma (guolim)
 *
 * Description: This is a migratable process. Given a number n, the process
 * 				tries to calculate the square root of n.
 */

package processes;

public class SqrtProcess implements MigratableProcess {
	private volatile boolean suspending;
	private double n = 0;
	private double k = 1.0;

	public SqrtProcess(String[] args) {
		this.n = Double.parseDouble(args[0]);
	}

	@Override
	public void run() {
		while (!this.suspending && Math.abs(k * k - n) > 1e-9) {
			k = (k + n / k) / 2;

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		System.out.println("sqrt of " + n + " is " + k);

		this.suspending = false;
	}

	@Override
	public void suspend() {
		this.suspending = true;
		while (this.suspending) {
		}
		;
	}
}