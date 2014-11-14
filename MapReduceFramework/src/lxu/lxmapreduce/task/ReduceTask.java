package lxu.lxmapreduce.task;

import lxu.lxdfs.metadata.LocatedBlock;
import lxu.lxmapreduce.tmp.JobConf;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by Wei on 11/13/14.
 */
public class ReduceTask extends Task {
	protected ReduceTask(TaskAttemptID attemptID, int partition, LocatedBlock locatedBlock) {
		super(attemptID, partition, locatedBlock);
	}

	@Override
	public void run(JobConf jobConf) throws IOException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, InstantiationException, IllegalAccessException {

	}
}
