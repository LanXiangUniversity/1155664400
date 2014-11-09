/**
 * ProcessState.java
 * @author Tong Wei (twei1), Guoli Ma (guolim)
 *
 * Description: The process state. Record the process ID, the process name, and
 * 				the slave node on which it is running.
 */

package master;

/**
 * Created by Wei on 9/6/14.
 * Modified by Ma on 9/10/14.
 */
public class ProcessState {
	private int pid;
	private String processName;
	private String nodeName;

	public ProcessState(int pid, String processName, String nodeName) {
		this.processName = processName;
		this.nodeName = nodeName;
		this.pid = pid;
	}

	public String getProcessName() {
		return processName;
	}

	public void setProcessName(String processName) {
		this.processName = processName;
	}

	public String getNodeName() {
		return nodeName;
	}

	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	public int getPid() {
		return pid;
	}

	public void setPid(int pid) {
		this.pid = pid;
	}
}
