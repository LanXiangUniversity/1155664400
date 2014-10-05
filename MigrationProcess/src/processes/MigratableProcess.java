/**
 * MigratableProcess.java
 * @author Tong Wei (xxxx), Guoli Ma (guolim)
 * 
 * Description: The MigratableProcess Interface. All migratable process must
 * 				implement this interface.
 */

package processes;

import java.io.Serializable;

public interface MigratableProcess extends Runnable, Serializable {
	public void suspend();
}
