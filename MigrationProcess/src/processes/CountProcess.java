/**
 * CountProcess.java
 * @author Tong Wei (twei1), Guoli Ma (guolim)
 * 
 * Description: This is a migratable process. Given a starting number, a count
 * 				step, and an upper bound, the process will continuously count 
 * 				until current number is larger than upper bound.
 */

package processes;

public class CountProcess implements MigratableProcess {
    private static final long serialVersionUID = -3881329126388008331L;
    private int current;
    private int step;
    private int upperBound;

    private volatile boolean suspending;

    public CountProcess(String[] args) throws Exception {
    	if (args.length != 3) {
			System.out.println("usage: CountProcess <start> <step> <upper bound>");
			throw new Exception("Invalid Arguments.");
		}
    	
        current = Integer.parseInt(args[0]);
        step = Integer.parseInt(args[1]);
        upperBound = Integer.parseInt(args[2]);
        suspending = false;
    }

    @Override
    public void run() {
        System.out.println("Start counting...");
        while (!suspending) {
            System.out.println(current);
            current += step;
            if (current > upperBound) {
            	break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            	// ignore it
            }
        }
        suspending = false;
    }

    public void suspend() {
		suspending = true;
		while (suspending);
    }
}
