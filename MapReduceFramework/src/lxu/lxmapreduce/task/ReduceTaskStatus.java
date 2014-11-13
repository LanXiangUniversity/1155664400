package lxu.lxmapreduce.task;

/**
 * Created by magl on 14/11/13.
 */
public class ReduceTaskStatus extends TaskStatus {
	@Override
	public boolean isMapTask() {
		return false;
	}
}
