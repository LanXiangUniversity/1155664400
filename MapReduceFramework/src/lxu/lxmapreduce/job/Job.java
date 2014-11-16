package lxu.lxmapreduce.job;

import lxu.lxmapreduce.tmp.Configuration;
import lxu.lxmapreduce.tmp.JobContext;

import java.io.IOException;

/**
 * Created by magl on 14/11/15.
 */
public class Job extends JobContext {
    private JobClient jobClient;
    private JobState state = JobState.DEFINE;
    private JobStatus status = null;

    public static enum JobState {
        DEFINE, RUNNING
    }

    public Job(Configuration conf) {
        super(conf, null);
    }

    // TODO: update job status using jobclient
    public void updateStatus() {

    }

    public JobStatus getStatus() throws IOException, InterruptedException {
        updateStatus();
        return status;
    }

    private void ensureState(JobState state) throws IllegalStateException {
        if (this.state != state) {
            throw new IllegalStateException("Job in state "+ this.state +
                                            " instead of " + state);
        }

        if (state == JobState.RUNNING && jobClient == null) {
            throw new IllegalStateException("Job in state " + JobState.RUNNING +
                                            " however jobClient is not initialized!");
        }
    }
}
