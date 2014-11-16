package lxu.lxmapreduce.task.map;

import lxu.lxmapreduce.task.TaskStatus;

/**
 * Created by magl on 14/11/13.
 */
public class MapTaskStatus extends TaskStatus {
	@Override
	public boolean isMapTask() {
		return true;
	}
}
